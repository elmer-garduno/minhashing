package mx.itam.metodos.mhclustering;

import mx.itam.metodos.common.ShingleKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopMinhashClustering extends Configured implements Tool {

  public static final String ROWS = "rows";
  public static final String TOP_K = "top-k";

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path data = new Path(otherArgs[0]);
    int rows = Integer.parseInt(otherArgs[2]);
    Path sketches = new Path(otherArgs[1] + "-sketches-" + rows);
    sketches.getFileSystem(conf).delete(sketches, true);
    Path clusters = new Path(otherArgs[1] + "-clusters-" + rows);
    clusters.getFileSystem(conf).delete(clusters, true);
    try {
      conf.setInt(ROWS, rows);
      Path out = new Path(otherArgs[1] + "-" + rows);
      computeMinhashes(data, sketches, conf);
      computeClusters(sketches, clusters, conf);
      groupClusters(clusters, out, conf);
    } finally {
      sketches.getFileSystem(conf).deleteOnExit(sketches);
      clusters.getFileSystem(conf).deleteOnExit(clusters);
    }
    return 0;
  }

  private static void computeMinhashes(Path data, Path out, Configuration conf) throws Exception {
    JobConf job = new JobConf(conf, HadoopMinhashClustering.class);
    job.setMapperClass(MinhashEmitMapper.class);
    job.setPartitionerClass(ShinglesPartitioner.class);
    job.setOutputValueGroupingComparator(GroupingComparator.class);
    job.setMapOutputKeyClass(ShingleKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(MinhashEmitReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }

  private static void computeClusters(Path data, Path out, Configuration conf) throws Exception {
    JobConf job = new JobConf(conf, HadoopMinhashClustering.class);
    job.setMapperClass(IdentityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(LSHClusterReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }
  

  private static void groupClusters(Path data, Path out, Configuration conf) throws Exception {
    JobConf job = new JobConf(conf, HadoopMinhashClustering.class);
    job.setMapperClass(IdentityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(IdentityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopMinhashClustering(), args);
    System.exit(res);
  }

  public static class ShinglesPartitioner implements Partitioner<ShingleKey, Text> {
    @Override
    public void configure(JobConf job) {
    }

    @Override
    public int getPartition(ShingleKey key, Text value, int numPartitions) {
      return Math.abs(key.getShingle().hashCode() * 127) % numPartitions;
    }
  }

  public static class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
      super(ShingleKey.class);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      ShingleKey sk1 = (ShingleKey) w1;
      ShingleKey sk2 = (ShingleKey) w2;
      return sk1.getShingle().compareTo(sk2.getShingle());
    }
  }
}
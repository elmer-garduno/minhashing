package mx.itam.metodos.lshclustering;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import mx.itam.metodos.common.SecondarySortKey;
import mx.itam.metodos.common.TextArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopLSHClustering extends Configured implements Tool {
  
  public static final String ROWS = "rows";
  public static final String BANDS = "bands";
  public static final String TOP_K = "top-k";

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Path data = new Path(args[0]);
    int rows = Integer.parseInt(args[2]);
    conf.setInt(ROWS, rows);
    int bands = Integer.parseInt(args[3]);
    conf.setInt(BANDS, bands);
    String suffix = String.format("%s-%s", rows, bands);
    Path sketches = new Path(args[1] + "-sketches-" + suffix);
    sketches.getFileSystem(conf).delete(sketches, true);
    Path clusters = new Path(args[1] + "-clusters-" + suffix);
    clusters.getFileSystem(conf).delete(clusters, true);
    try {
      Path out = new Path(args[1] + "-" + suffix);
      out.getFileSystem(conf).delete(out, true);
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
    JobConf job = new JobConf(conf, HadoopLSHClustering.class);
    job.setMapperClass(MinhashEmitMapper.class);
    job.setPartitionerClass(SecondarySortKey.KeyPartitioner.class);
    job.setOutputValueGroupingComparator(SecondarySortKey.GroupingComparator.class);
    job.setMapOutputKeyClass(SecondarySortKey.class);
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
    JobConf job = new JobConf(conf, HadoopLSHClustering.class);
    job.setMapperClass(IdentityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(LSHClusterReducer.class);
    job.setOutputKeyClass(SecondarySortKey.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }
  

  private static void groupClusters(Path data, Path out, Configuration conf) throws Exception {
    JobConf job = new JobConf(conf, HadoopLSHClustering.class);
    job.setMapperClass(IdentityMapper.class);
    job.setPartitionerClass(SecondarySortKey.KeyPartitioner.class);
    job.setOutputValueGroupingComparator(SecondarySortKey.GroupingComparator.class);
    job.setMapOutputKeyClass(SecondarySortKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(GroupInMemoryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextArrayWritable.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopLSHClustering(), args);
    System.exit(res);
  }
}
package mx.itam.metodos.shingles;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopShingles extends Configured implements Tool{

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Path data = new Path(args[0]);
    Path out = new Path(args[1]);
    conf.setInt("k", Integer.parseInt(args[2]));
    computeShingles(data, out, conf);
    return 0;
  }

  private static void computeShingles(Path data, Path out, Configuration conf) throws Exception {
    JobConf job = new JobConf(conf, HadoopShingles.class);
    job.setJarByClass(HadoopShingles.class);
    job.setMapperClass(ShinglesMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(ShinglesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    JobClient.runJob(job);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopShingles(), args);
    System.exit(res);
  }
}
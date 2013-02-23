package mx.itam.metodos.minhashing;

import mx.itam.metodos.common.TextArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopMinhashing {
  
  public static final String ROWS = "rows";

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(HadoopMinhashing.class);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path data = new Path(otherArgs[0]);
    Path tmp = new Path("tmp" + Math.random());
    try {
      int rows = Integer.parseInt(otherArgs[2]);
      conf.setInt(ROWS, rows);
      Path out = new Path(otherArgs[1] + "-" + rows);
      computeMinhashes(data, tmp, conf);
      computeClusters(tmp, out, conf);    
    } finally {
      tmp.getFileSystem(conf).deleteOnExit(tmp);
    }
  }

  private static void computeMinhashes(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-minhashing");
    job.setJarByClass(HadoopMinhashing.class);
    job.setMapperClass(MinhashMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(MinhashReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextArrayWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    job.waitForCompletion(true);
  }
  
  private static void computeClusters(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-minhashing");
    job.setJarByClass(HadoopMinhashing.class);
    job.setMapperClass(LSHSimilarityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(LSHSimilarityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    job.waitForCompletion(true);
  }
}
package mx.itam.metodos.shingles;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopShingles {

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(HadoopShingles.class);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path data = new Path(otherArgs[0]);
    Path out = new Path(otherArgs[1]);

    //conf.setInt("k", Integer.parseInt(otherArgs[2]));
    computeShingles(data, out, conf);
  }

  private static void computeShingles(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-shingles");

    job.setJarByClass(HadoopShingles.class);
    job.setMapperClass(ShinglesMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(ShinglesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);

    job.waitForCompletion(true);
  }
}
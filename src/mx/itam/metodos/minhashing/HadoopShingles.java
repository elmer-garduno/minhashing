package mx.itam.metodos.minhashing;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopShingles {
  public enum NBCounters {
    CORRECT, COUNT;
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(HadoopShingles.class);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path data = new Path(otherArgs[0]);
    Path out = new Path(otherArgs[1]);
    //Path model = new Path(out, "train");
    //conf.set("model-path", FSHelper.getQualified(model, conf));
    computeShingles(data, out, conf);
//    Path test = new Path(args[1]);
//    Path vocab = new Path(out, "vocab");
//    conf.set("vocab-path", FSHelper.getQualified(vocab, conf));
//    buildVocab(test, vocab, conf);
//    Path result = new Path(out, "test");
//    test(test, result, conf);
  }

  private static void computeShingles(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-nb-train");

    job.setJarByClass(HadoopShingles.class);
    job.setMapperClass(ShinglesMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
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
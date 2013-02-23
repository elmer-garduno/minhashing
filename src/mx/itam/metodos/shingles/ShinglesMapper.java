package mx.itam.metodos.shingles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public final class ShinglesMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

  @Override
  public void map(Text key, Text value, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
    String text = value.toString();
    int k = 10;
    IntWritable shingle = new IntWritable();
    for (int i = 0; i < text.length() - k ; i++) {
      shingle.set(text.substring(i, i + k).hashCode());
      collector.collect(key, shingle);
    }
  }
}

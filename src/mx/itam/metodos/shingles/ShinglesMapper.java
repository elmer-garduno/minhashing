package mx.itam.metodos.shingles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class ShinglesMapper extends Mapper<Text, Text, Text, IntWritable> {

  @Override
  public void map(Text key, Text value, Context ctx) throws IOException,
          InterruptedException {
    String text = value.toString();
    int k = 10;
    IntWritable shingle = new IntWritable();
    for (int i = 0; i < text.length() - k ; i++) {
      shingle.set(text.substring(i, i + k).hashCode());
      ctx.write(key, shingle);
    }
  }

}

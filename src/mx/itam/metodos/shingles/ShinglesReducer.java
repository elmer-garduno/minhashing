package mx.itam.metodos.shingles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ShinglesReducer extends MapReduceBase implements
 Reducer<Text, IntWritable, Text, IntArrayWritable> {
 
  @Override
  public void reduce(Text key, Iterator<IntWritable> values,
          OutputCollector<Text, IntArrayWritable> collector, Reporter reporter) throws IOException {
     List<IntWritable> shingles = new ArrayList<IntWritable>();
     while(values.hasNext()) {
      IntWritable x = values.next();
      IntWritable shingle = new IntWritable();
      shingle.set(x.get());
      shingles.add(shingle);
    }
    IntArrayWritable out = new IntArrayWritable();
    out.set(shingles.toArray(new IntWritable[0]));
    collector.collect(key, out);
  }
}

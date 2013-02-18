package mx.itam.metodos.minhashing;

//This class is based on the method for LSH explained on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.List;

import mx.itam.metodos.common.TextArrayWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class MinhashReducer extends
 Reducer<Text, Text, Text, TextArrayWritable> {
  
  @Override
  public void reduce(Text id, Iterable<Text> values, Context ctx) 
    throws IOException, InterruptedException {
    List<Text> documents = Lists.newArrayList();
    for (Text x : values) {
      documents.add(new Text(x));
    }
    if (documents.size() > 1) {
      TextArrayWritable out = new TextArrayWritable();
      out.set(documents.toArray(new Text[0]));
      ctx.write(new Text(id), out);
    }
  }
}

package mx.itam.metodos.tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


public class GroupReducer extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context ctx) 
    throws IOException, InterruptedException {
    List<String> data = Lists.newArrayList();
    for (Text x : values) {
     data.add(x.toString());
    }
    Joiner joiner = Joiner.on(" ");
    ctx.write(key, new Text(joiner.join(data)));
  }
}

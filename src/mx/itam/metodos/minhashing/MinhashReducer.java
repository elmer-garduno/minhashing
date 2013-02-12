package mx.itam.metodos.minhashing;

//This class is based on the org.apache.mahout.clustering.minhash.MinHashMapper
//available under the Apache License 2.0
//This version has been simplified for educational purposes

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class MinhashReducer extends
 Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text id, Iterable<Text> values, Context ctx) 
    throws IOException, InterruptedException {
    List<Text> cluster = Lists.newArrayList();
    for (Text x : values) {
      Text document = new Text(x);
      cluster.add(document);
    }
    if (cluster.size() > 2) {
      for (Text doc : cluster) {
        ctx.write(id, doc);
      }
    }
  }
}

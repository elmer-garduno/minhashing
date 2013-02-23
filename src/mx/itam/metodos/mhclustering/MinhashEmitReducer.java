package mx.itam.metodos.mhclustering;

//This class is based on the method for LSH explained on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mx.itam.metodos.common.ShingleKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Lists;

// Receives pairs<ShingleKey, Iterable<ID>>
public class MinhashEmitReducer extends MapReduceBase implements Reducer <ShingleKey, Text, Text, Text> {
  
  @Override
  public void reduce(ShingleKey key, Iterator<Text> ids, OutputCollector<Text, Text> collector,
          Reporter reporter) throws IOException {
    List<Text> documents = Lists.newArrayList();
    Text first = null;
    while (ids.hasNext()) {
      Text x = new Text(ids.next());
      if (first == null) {
        first = x;
      }
      for (Text text : documents) {
        collector.collect(new Text(String.format("%s-%s", text, x)), first);
      }
      documents.add(x);
    }
  }
}

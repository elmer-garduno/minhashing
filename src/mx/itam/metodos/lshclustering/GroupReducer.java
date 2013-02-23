package mx.itam.metodos.lshclustering;

import java.io.IOException;
import java.util.Iterator;

import mx.itam.metodos.common.SecondarySortKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GroupReducer extends MapReduceBase implements
        Reducer<SecondarySortKey, Text, Text, Text> {

  @Override
  public void reduce(SecondarySortKey cluster, Iterator<Text> ids,
          OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
    Text label = null;
    while (ids.hasNext()) {
      Text str = new Text(ids.next());
      if (label != null && !label.equals(str)) {
        collector.collect(cluster.getKey(), new Text(label));
      }
      label = str;
    }        
    collector.collect(cluster.getKey(), new Text(label));
  }
}

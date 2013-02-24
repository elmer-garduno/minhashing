package mx.itam.metodos.lshclustering;

import java.io.IOException;

import mx.itam.metodos.common.SecondarySortKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupReducer extends Reducer<SecondarySortKey, Text, Text, Text> {

  @Override
  public void reduce(SecondarySortKey cluster, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
    Text label = null;
    for (Text txt : ids) {
      Text str = new Text(txt);
      if (label != null && !label.equals(str)) {
        context.write(cluster.getKey(), new Text(label));
      }
      label = str;
    }        
    context.write(cluster.getKey(), new Text(label));
  }
}

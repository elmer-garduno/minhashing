package mx.itam.metodos.mhclustering;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import mx.itam.metodos.common.SecondarySortKey;
import mx.itam.metodos.common.TextArrayWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Sets;

public class GroupInMemoryReducer extends MapReduceBase implements
        Reducer<SecondarySortKey, Text, Text, TextArrayWritable> {


  @Override
  public void reduce(SecondarySortKey cluster, Iterator<Text> ids,
          OutputCollector<Text, TextArrayWritable> collector, Reporter reporter) throws IOException {
    Text uuid = new Text(UUID.randomUUID().toString());
    Set<Text> clustered = Sets.newLinkedHashSet();
    while (ids.hasNext()) {
      Text str = new Text(ids.next());
      clustered.add(str);
    }        
    TextArrayWritable out = new TextArrayWritable();
    out.set(clustered.toArray(new Text[0]));
    collector.collect(uuid,out);
  }
}

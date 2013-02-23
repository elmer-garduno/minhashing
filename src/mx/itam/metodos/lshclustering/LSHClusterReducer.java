package mx.itam.metodos.lshclustering;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mx.itam.metodos.common.SecondarySortKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class LSHClusterReducer extends MapReduceBase implements
        Reducer<Text, Text, SecondarySortKey, Text> {

  private final Logger logger = Logger.getLogger(LSHClusterReducer.class);

  private float bands;

  private float threshold;
  
  private int topk;

  @Override
  public void reduce(Text pair, Iterator<Text> sketches,
          OutputCollector<SecondarySortKey, Text> collector, Reporter reporter) throws IOException {
    int count = 0;
    Set<String> set = Sets.newHashSet();
    while (sketches.hasNext()) {
      Text sketch = sketches.next();
      count++;
      set.add(sketch.toString());
    }
    List<String> top = Ordering.natural().leastOf(set, topk);
    float fraction = count / bands;
    if (fraction > threshold) {
      String[] values = pair.toString().split("\\|");
      for (String str : top) {
        Text key = new Text(str);
        Text v0 = new Text(values[0]);
        collector.collect(new SecondarySortKey(key, v0), v0);
        Text v1 = new Text(values[1]);
        collector.collect(new SecondarySortKey(key, v1), v1);
      }
    }
  }

  @Override
  public void configure(JobConf job) {
    int functionsCount = 100;
    int rows = job.getInt(HadoopLSHClustering.ROWS, 10);
    this.topk = job.getInt(HadoopLSHClustering.TOP_K, 1);
    this.bands = functionsCount / rows;
    this.threshold = (float) Math.pow(1 / bands, 1 / (float) rows);
    logger.info(String.format("{b:%s, r:%s, t:%.4f}", bands, rows, threshold));
  }
}

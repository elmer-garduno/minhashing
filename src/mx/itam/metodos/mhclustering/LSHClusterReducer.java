package mx.itam.metodos.mhclustering;

//This class is based on the method for LSH descibed on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class LSHClusterReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

  private final Logger logger = Logger.getLogger(LSHClusterReducer.class);

  private float bands;

  private float threshold;
  
  private int topk;

  @Override
  public void reduce(Text pair, Iterator<Text> sketches,
          OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
    int count = 0;
    //List<String> all = Lists.newArrayList();
    Set<String> set = Sets.newHashSet();
    while (sketches.hasNext()) {
      Text sketch = sketches.next();
      count++;
      set.add(sketch.toString());
    }
    List<String> top = Ordering.natural().sortedCopy(set);//.leastOf(all, topk);
    Joiner joiner = Joiner.on(",");
    float fraction = count / bands;
    if (fraction > threshold) {
      String[] values = pair.toString().split("-");
      for (String str : top) {
        collector.collect(new Text(str), new Text(values[0]));
        collector.collect(new Text(str), new Text(values[1]));
      }
//      List<Text> documents = Lists.newArrayList();
//      for (String str : top) {
//        Text x = new Text(str);
//        for (Text text : documents) {
//          collector.collect(new Text(String.format("%s-%s", text, x)), pair);
//        }
//        documents.add(x);
//      }
      //collector.collect(new Text(String.format("%s->%s",pair, top)), new FloatWritable(fraction));
    }
  }

  @Override
  public void configure(JobConf job) {
    int functionsCount = 100;
    this.topk = job.getInt(HadoopMinhashClustering.TOP_K, 4);
    int rows = job.getInt(HadoopMinhashClustering.ROWS, 10);
    this.bands = functionsCount / rows;
    this.threshold = (float) Math.pow(1 / bands, 1 / (float) rows);
    logger.info(String.format("{b:%s, r:%s, t:%.4f}", bands, rows, threshold));
  }
}

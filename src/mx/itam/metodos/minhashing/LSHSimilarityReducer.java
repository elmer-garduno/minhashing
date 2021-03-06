package mx.itam.metodos.minhashing;

// This method is based on Broder '97 Syntactic Clustering of the Web 
// plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class LSHSimilarityReducer extends
 Reducer<Text, Text, Text, FloatWritable> {

  private final Logger logger = Logger.getLogger(LSHSimilarityReducer.class); 
  
  private float bands;
  
  private float threshold;
  
  private Multiset<String> counters;
  
  @Override
  public void reduce(Text id, Iterable<Text> values, Context ctx) 
    throws IOException, InterruptedException {
    counters = HashMultiset.create();
    String a = id.toString();
    for (Text x : values) {
      counters.add(x.toString());
    }
    for (Multiset.Entry<String> entry : counters.entrySet()) {
      float fraction = entry.getCount() / bands;
      if (fraction > threshold) {
        String b = entry.getElement();
        ctx.write(new Text(String.format("%s-%s", a, b)), new FloatWritable(fraction));
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    int functionsCount = 100;
    int rows = context.getConfiguration().getInt(HadoopMinhashing.ROWS, 10);
    this.bands = functionsCount / rows;
    this.threshold = (float) Math.pow(1 / bands, 1 / (float) rows);
    logger.info(String.format("{b:%s, r:%s, t:%.4f}", bands, rows, threshold));
  }
}

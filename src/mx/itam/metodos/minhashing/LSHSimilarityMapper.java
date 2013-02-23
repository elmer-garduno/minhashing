package mx.itam.metodos.minhashing;

// This method is based on Broder '97 Syntactic Clustering of the Web 
// plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mx.itam.metodos.common.TextArrayWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;

public final class LSHSimilarityMapper extends Mapper<Text, TextArrayWritable, Text, Text> {

  @Override
  public void map(Text key, TextArrayWritable values, Context ctx) throws IOException,
          InterruptedException {
    List<Text> data = Lists.newArrayList();
    for (Writable wr : values.get()) {
      data.add((Text) wr);
    }
    Collections.sort(data);      
    for (Iterator<Text> it = data.iterator(); it.hasNext(); ) {
      Text cur = it.next();
      it.remove();
      for (Text doc : data) {
        ctx.write(cur, doc);
      }
    }
  }
}

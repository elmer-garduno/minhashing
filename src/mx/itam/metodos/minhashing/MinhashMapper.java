// This class is based on the org.apache.mahout.clustering.minhash.MinHashMapper
// available under the Apache License 2.0
// and on the method for LSH explained on Rajaraman, Leskovec and Ullman 2012

package mx.itam.metodos.minhashing;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public final class MinhashMapper extends Mapper<Text, IntArrayWritable, Text, Text> {

  private HashFunction lsh;
  
  private HashFunction[] functions;

  private int functionsCount;

  private int rows;
  
  private int[] hashValues; 

  @Override
  public void map(Text key, IntArrayWritable values, Context ctx) throws IOException,
          InterruptedException {
    for (int i = 0; i < functionsCount; i++) {
      hashValues[i] = Integer.MAX_VALUE;
    }
    for (int i = 0; i < functionsCount; i++) {
      HashFunction hf = functions[i];
      for (Writable wr : values.get()) {
        IntWritable value = (IntWritable) wr;
        int hash = hf.hashInt(value.get()).asInt();
        if (hash < hashValues[i]) {
          hashValues[i] = hash;
        }
      }
    }
    Text text = new Text();
    Hasher hasher = lsh.newHasher();
    int band = 0;
    for (int i = 0; i < functionsCount; i++) { 
      hasher.putInt(hashValues[i]);
      if (i > 0 && (i % rows) == 0) {
        text.set(band + "-" + hasher.hash().toString());
        write(key, text, ctx);
        hasher = lsh.newHasher();
        band++;
      }
    }
    text.set(band + "-" + hasher.hash().toString());
    write(key, text, ctx);
  }
  
  private void write(Text key, Text text, Context ctx) throws IOException, InterruptedException {
    ctx.write(text, key);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.functionsCount = 100;
    this.rows = context.getConfiguration().getInt(HadoopMinhashing.ROWS, 10);
    this.hashValues = new int[functionsCount];
    this.functions = new HashFunction[functionsCount];
    Random r = new Random(11);
    for (int i = 0; i < functionsCount; i++) {
      functions[i] = Hashing.murmur3_32(r.nextInt());
    }
    this.lsh = Hashing.murmur3_32(r.nextInt());
  }
}

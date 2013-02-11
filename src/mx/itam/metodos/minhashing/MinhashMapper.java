// This class is based on the org.apache.mahout.clustering.minhash.MinHashMapper
// available under the Apache License 2.0
// This version has been simplified for educational purposes

package mx.itam.metodos.minhashing;

import java.io.IOException;
import java.util.Random;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public final class MinhashMapper extends Mapper<Text, IntArrayWritable, Text, Text> {

  private HashFunction[] functions;

  private int functionsCount;
  
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
    for (int i = 0; i < functionsCount; i++) { 
      ctx.write(new Text(String.valueOf(hashValues[i])), key);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.functionsCount = 10;
    this.hashValues = new int[functionsCount];
    this.functions = new HashFunction[functionsCount];
    Random r = new Random(11);
    for (int i = 0; i < functionsCount; i++) {
      functions[i] = Hashing.murmur3_32(r.nextInt());
    }
  }
}

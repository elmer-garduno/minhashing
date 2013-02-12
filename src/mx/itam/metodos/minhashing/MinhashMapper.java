// This class is based on the org.apache.mahout.clustering.minhash.MinHashMapper
// available under the Apache License 2.0
// This version has been simplified for educational purposes

package mx.itam.metodos.minhashing;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import mx.itam.metodos.common.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public final class MinhashMapper extends Mapper<Text, IntArrayWritable, Text, Text> {

  private HashFunction[] functions;

  private int functionsCount;

  private int groups;
  
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
    Joiner joiner = Joiner.on("-");
    for (int i = 0; i < functionsCount; i++) { 
      List<String> str = Lists.newArrayList();
      for (int j = 0; j < groups; j++) {
        str.add(String.valueOf(hashValues[(i + j) % functionsCount]));
      }
      ctx.write(new Text(joiner.join(str)), key);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.functionsCount = 100;
    this.groups = 7;
    this.hashValues = new int[functionsCount];
    this.functions = new HashFunction[functionsCount];
    Random r = new Random(11);
    for (int i = 0; i < functionsCount; i++) {
      functions[i] = Hashing.murmur3_32(r.nextInt());
    }
  }
}

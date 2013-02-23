package mx.itam.metodos.mhclustering;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012
//and code originally found on org.apache.mahout.clustering.minhash.MinHashMapper
//available under the Apache License 2.0.

import java.io.IOException;
import java.util.Random;

import mx.itam.metodos.common.IntArrayWritable;
import mx.itam.metodos.common.SecondarySortKey;
import mx.itam.metodos.minhashing.HadoopMinhashing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public final class MinhashEmitMapper extends MapReduceBase implements Mapper<Text, IntArrayWritable, SecondarySortKey, Text> {

  private HashFunction lsh;
  
  private HashFunction[] functions;

  private int functionsCount;

  private int rows;
  
  private int[] hashValues; 

  @Override
  public void map(Text id, IntArrayWritable values, OutputCollector<SecondarySortKey, Text> output, Reporter reporter) throws IOException {
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
    Text sketch = new Text();
    Hasher hasher = lsh.newHasher();
    int band = 0;
    for (int i = 0; i < functionsCount; i++) { 
      hasher.putInt(hashValues[i]);
      if (i > 0 && (i % rows) == 0) {
        sketch.set(band + "-" + hasher.hash().toString());
        output.collect(new SecondarySortKey(sketch, id), id);
        hasher = lsh.newHasher();
        band++;
      }
    }
    sketch.set(band + "-" + hasher.hash().toString());
    output.collect(new SecondarySortKey(sketch, id), id);
  }
  
  @Override
  public void configure(JobConf job) {
    this.functionsCount = 100;
    this.rows = job.getInt(HadoopMinhashing.ROWS, 10);
    this.hashValues = new int[functionsCount];
    this.functions = new HashFunction[functionsCount];
    Random r = new Random(11);
    for (int i = 0; i < functionsCount; i++) {
      functions[i] = Hashing.murmur3_32(r.nextInt());
    }
    this.lsh = Hashing.murmur3_32(r.nextInt());
  }
}

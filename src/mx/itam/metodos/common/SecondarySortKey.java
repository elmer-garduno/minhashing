package mx.itam.metodos.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ShingleKey implements WritableComparable<ShingleKey> {
    private Text shingle;
    private Text id;
    
    public ShingleKey(Text shingle, Text id) {
      this.shingle = shingle;
      this.id = id;
    }

    public ShingleKey() {
      this(new Text(), new Text());
    }

    public Text getShingle() {
      return shingle;
    }
    
    public Text getId() {
      return id;
    }
    
    public void write(DataOutput out) throws IOException {
      id.write(out);
      shingle.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      shingle.readFields(in);
    }
    
    public int compareTo(ShingleKey o) {
      int diff = shingle.compareTo(o.shingle);
      if (diff != 0) {
        return diff;
      } 
      return id.compareTo(o.id); 
    }

    public String toString() {
      return shingle + ":" + id;
    }
  }

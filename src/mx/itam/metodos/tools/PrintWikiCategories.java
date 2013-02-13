package mx.itam.metodos.tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class PrintWikiCategories { 

  private static void group(SequenceFile.Reader reader) throws IOException {
    Joiner joiner = Joiner.on(" ");
    Text cluster = new Text();
    Text article = new Text();
    String prevKey = null;
    List<String> list = Lists.newArrayList();
    while (reader.next(cluster, article)) {
      String key = cluster.toString();
      if (prevKey != null && !prevKey.equals(key)) {
        System.out.printf("%s\n\n", joiner.join(list));
        list = Lists.newArrayList();
      }
      list.add(article.toString());
      prevKey = key;
    }
    System.out.printf("%s\n\n", joiner.join(list));
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path file = new Path(args[0]);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    group(reader);
  }
}

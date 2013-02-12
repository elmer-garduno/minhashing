package mx.itam.metodos.tools;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import mx.itam.metodos.common.SplitBuilder;
import mx.itam.metodos.common.StreamingCorpusReader;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class GroupWikiCategories { 

  private static void group(StreamingCorpusReader<String[]> reader) {
    String prevKey = null;
    Joiner joiner = Joiner.on(" ");
    List<String> list = Lists.newArrayList();
    for (String[] values : reader) {
      String key = values[0];
      if (prevKey != null && !prevKey.equals(key)) {
        System.out.printf("%s\t%s\n", prevKey, joiner.join(list));
        list = Lists.newArrayList();
      }
      list.add(values[1]);
      prevKey = key;
    }
    System.out.printf("%s\t%s\n", prevKey, joiner.join(list));
  }

  public static void main(String[] args) throws Exception {
    Reader in = new InputStreamReader(System.in);
    StreamingCorpusReader<String[]> reader = new StreamingCorpusReader<String[]>(in,
            new SplitBuilder(" "));
    group(reader);
  }
}

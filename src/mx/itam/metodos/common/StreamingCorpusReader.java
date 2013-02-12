package mx.itam.metodos.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;


public final class StreamingCorpusReader<T> implements Iterable<T> {
  
  private final BufferedReader reader;
  
  private final RecordBuilder<T> builder;
  
  public StreamingCorpusReader(Reader reader, RecordBuilder<T> builder) {
    this.reader = new BufferedReader(reader);
    this.builder = builder;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      
      private String line;
      
      @Override
      public boolean hasNext() {
        try {
          return (line = reader.readLine()) != null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T next() {
        return builder.build(line);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }
  
}

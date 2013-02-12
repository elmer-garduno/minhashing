package mx.itam.metodos.common;

public class SplitBuilder implements RecordBuilder<String[]> {

  private final String sep;
  
  public SplitBuilder(String sep) {
    this.sep = sep;
  }
  
  public String[] build(String line) {
    return line.split(sep);
  }
}

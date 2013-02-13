package mx.itam.metodos.common;

public interface RecordBuilder<T> {

  T build(String line);

}

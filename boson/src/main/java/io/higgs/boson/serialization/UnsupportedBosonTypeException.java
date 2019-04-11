package io.higgs.boson.serialization;

public class UnsupportedBosonTypeException extends RuntimeException {
  public UnsupportedBosonTypeException(String msg, Throwable cause) {
    super(msg, cause);
  }
}

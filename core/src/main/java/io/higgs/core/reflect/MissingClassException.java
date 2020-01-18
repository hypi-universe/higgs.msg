package io.higgs.core.reflect;

public class MissingClassException extends RuntimeException {
  public MissingClassException(final ClassNotFoundException cause) {
    super(cause);
  }
}

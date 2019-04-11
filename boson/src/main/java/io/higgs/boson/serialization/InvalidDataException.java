package io.higgs.boson.serialization;

public class InvalidDataException extends RuntimeException {

    public InvalidDataException(String msg, Throwable c) {
        super(msg, c);
    }
}

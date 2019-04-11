package io.higgs.boson.serialization;

public class CircularReferenceA {
    CircularReferenceB b;

    public CircularReferenceA(final CircularReferenceB b) {
        this.b = b;
    }

    public CircularReferenceA() {
        //keep serializer happy
    }

    public String toString() {
        return hashCode() + "-A";
    }
}

package io.higgs.boson.serialization;

/**
 * @author Courtney Robinson <courtney@crlog.info>
 */
public class CircularReferenceB {
    CircularReferenceA a;

    public void init() {
        a = new CircularReferenceA(this);
    }

    public String toString() {
        return hashCode() + "-B";
    }
}

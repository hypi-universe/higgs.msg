package io.higgs.boson;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Defines all the Boson types
 */
public enum BosonType {
  BYTE(1),
  SHORT(2),
  INT(3),
  LONG(4),
  FLOAT(5),
  DOUBLE(6),
  BOOLEAN(7),
  CHAR(8),
  NULL(9),
  STRING(10),
  ARRAY(11),
  LIST(12),
  MAP(13),
  POLO(14),
  REFERENCE(15),
  SET(16),
  ENUM(17),
  BYTE_ARRAY(18),
  //there are at least 15 sub-types of Java's temporal in JDK 11 - add them in the future
  DATE(19),
  LOCAL_DATE(20),
  LOCAL_DATETIME(21),
  LOCALTIME(22),
  DURATION(23),
  PERIOD(24),
  //reserved
  JODA_DATETIME(25),
  JODA_LOCAL_DATE(26),
  JODA_LOCALTIME(27),
  JODA_LOCAL_DATE_TIME(28),
  JODA_DURATION(29),
  JODA_INTERVAL(30),
  JODA_PERIOD(31),
  UUID(32);
  public final byte id;
  private static BosonType[] values;

  BosonType(int idx) {
    id = (byte) idx;
  }

  public static BosonType byId(byte id) {
    if (values == null) {
      values = values();
      Arrays.sort(values, Comparator.comparingInt(a -> a.id));
    }
    return values[id - 1];
  }
}

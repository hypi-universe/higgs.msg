package io.higgs.boson.serialization;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.higgs.boson.serialization.BosonReader.decode;
import static io.higgs.boson.serialization.BosonWriter.encode;
import static java.util.Collections.singleton;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Courtney Robinson <courtney@crlog.info>
 */
public class BosonWriterTest {
  @Test
  public void testWriteMap() throws Exception {
    String intStr = "int";
    Map<String, Map<String, Object>> map = new HashMap<>();
    Map intMap = new HashMap<>();
    Map longMap = new HashMap<>();
    Map stringMap = new HashMap<>();
    Map mixed = new HashMap<>();
    mixed.put("int", 1);
    mixed.put("int-array", new int[]{3, 4, 2, 5});
    mixed.put("long", 2L);
    mixed.put("byte", (byte) 3);
    mixed.put("short", (short) 4);
    mixed.put("boolean", true);
    mixed.put("byte[]", new byte[]{1, 2, 3});
    mixed.put("float", 5.3f);
    mixed.put("double", 6.2d);
    mixed.put("char", 'z');
    mixed.put("null", null);
    mixed.put("str", "a str");
    mixed.put("array", new Object[]{1, "2"});
    mixed.put("list", Arrays.asList(5, 6));
    mixed.put("set", singleton(34));

    //
    map.put("int", intMap);
    map.put("long", longMap);
    map.put("string", stringMap);
    map.put("mixed", mixed);
    //
    intMap.put("a", 1);
    intMap.put("b", 2);
    longMap.put("a", 1L);
    longMap.put("b", 2L);
    longMap.put("c", 3L);
    stringMap.put("a", "a");
    stringMap.put("b", "ab");
    stringMap.put("c", "abc");
    byte[] bytes = encode(map);
    Map<String, Map<String, Object>> out = decode(bytes);
    Map<String, Object> outInt = out.get("int");
    assertEquals(1, outInt.get("a"));
    assertEquals(2, outInt.get("b"));

    Map<String, Object> outLong = out.get("long");
    assertEquals(1L, outLong.get("a"));
    assertEquals(2L, outLong.get("b"));
    assertEquals(3L, outLong.get("c"));

    Map<String, Object> outString = out.get("string");
    assertEquals("a", outString.get("a"));
    assertEquals("ab", outString.get("b"));
    assertEquals("abc", outString.get("c"));

    Map<String, Object> outMixed = out.get("mixed");
    assertEquals(1, outMixed.get("int"));
    assertEquals(2L, outMixed.get("long"));
    assertEquals((byte) 3, outMixed.get("byte"));
    assertEquals((short) 4, outMixed.get("short"));
    assertEquals(true, outMixed.get("boolean"));
    assertEquals(5.3f, outMixed.get("float"));
    assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) outMixed.get("byte[]"));
    assertEquals(6.2d, outMixed.get("double"));
    assertEquals('z', outMixed.get("char"));
    assertNull(outMixed.get("null"));
    assertEquals("a str", outMixed.get("str"));
    assertArrayEquals(new Object[]{1, "2"}, (Object[]) outMixed.get("array"));
    assertEquals(Arrays.asList(5, 6), outMixed.get("list"));
    assertEquals(singleton(34), outMixed.get("set"));
  }

  @Test
  public void testDates() throws Exception {
    Date expected = new Date();
    Date actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testLocalDate() throws Exception {
    LocalDate expected = LocalDate.now();
    LocalDate actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testLocalDateTime() throws Exception {
    LocalDateTime expected = LocalDateTime.now();
    LocalDateTime actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testLocalTime() throws Exception {
    LocalTime expected = LocalTime.now();
    LocalTime actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testDuration() throws Exception {
    Duration expected = Duration.ofMinutes(10);
    Duration actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testInterval() throws Exception {
    Period expected = Period.ofDays(17);
    Period actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaDateTime() throws Exception {
    DateTime expected = DateTime.now();
    DateTime actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaLocalDate() throws Exception {
    org.joda.time.LocalDate expected = org.joda.time.LocalDate.now();
    org.joda.time.LocalDate actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaLocalDateTime() throws Exception {
    org.joda.time.LocalDateTime expected = org.joda.time.LocalDateTime.now();
    org.joda.time.LocalDateTime actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaLocalTime() throws Exception {
    org.joda.time.LocalTime expected = org.joda.time.LocalTime.now();
    org.joda.time.LocalTime actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaDuration() throws Exception {
    org.joda.time.Duration expected = org.joda.time.Duration.standardDays(24);
    org.joda.time.Duration actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testJodaInterval() throws Exception {
    Interval expected = new Interval(DateTime.now().minusHours(36), DateTime.now());
    Interval actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testPeriod() throws Exception {
    org.joda.time.Period expected = org.joda.time.Period.months(35);
    org.joda.time.Period actual = decode(encode(expected));
    assertEquals(expected, actual);
  }

  @Test
  public void testSerializingEnum() throws Exception {
    EnumEnclosingType obj = new EnumEnclosingType(SomeType.B);
    byte[] out = encode(obj);
    EnumEnclosingType in = decode(out);
    assertNotNull(in);
    assertEquals(obj.value, in.value);
  }

  @Test
  public void testSerializingEnumInNestedPOLO() throws Exception {
    OuterEnclosingType obj = new OuterEnclosingType();
    byte[] out = encode(obj);
    OuterEnclosingType in = decode(out);
    assertNotNull(in);
    assertEquals(obj.type.value, in.type.value);
  }

  enum SomeType {
    A, B, C
  }

  public static class EnumEnclosingType {
    private SomeType value;

    EnumEnclosingType(SomeType value) {
      this.value = value;
    }

    EnumEnclosingType() {
    }
  }

  public static class OuterEnclosingType {
    private EnumEnclosingType type = new EnumEnclosingType(SomeType.C);
  }
}

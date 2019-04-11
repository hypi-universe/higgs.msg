package io.higgs.boson.serialization;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
    BosonWriter writer = new BosonWriter();
    BosonReader reader = new BosonReader();
    String intStr = "int";
    Map<String, Map<String, Object>> map = new HashMap<>();
    Map intMap = new HashMap<>();
    Map longMap = new HashMap<>();
    Map stringMap = new HashMap<>();
    Map mixed = new HashMap<>();
    mixed.put("int", 1);
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
    byte[] bytes = writer.serialize(map);
    Map<String, Map<String, Object>> out = reader.deSerialise(bytes);
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
  public void testSerializingEnum() throws Exception {
    BosonWriter writer = new BosonWriter();
    BosonReader reader = new BosonReader();
    EnumEnclosingType obj = new EnumEnclosingType(SomeType.B);
    byte[] out = writer.serialize(obj);
    EnumEnclosingType in = reader.deSerialise(out);
    assertNotNull(in);
    assertEquals(obj.value, in.value);
  }

  @Test
  public void testSerializingEnumInNestedPOLO() throws Exception {
    BosonWriter writer = new BosonWriter();
    BosonReader reader = new BosonReader();
    OuterEnclosingType obj = new OuterEnclosingType();
    byte[] out = writer.serialize(obj);
    OuterEnclosingType in = reader.deSerialise(out);
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

package io.higgs.core.reflect;

import org.joda.time.DateTime;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static io.higgs.core.reflect.ReflectionUtil.dotPath;
import static io.higgs.core.reflect.ReflectionUtil.field;
import static io.higgs.core.reflect.ReflectionUtil.getAllFields;
import static io.higgs.core.reflect.ReflectionUtil.getAllMethods;
import static io.higgs.core.reflect.ReflectionUtil.getFieldOrGetterType;
import static io.higgs.core.reflect.ReflectionUtil.isBigDecimal;
import static io.higgs.core.reflect.ReflectionUtil.isBigInt;
import static io.higgs.core.reflect.ReflectionUtil.isBool;
import static io.higgs.core.reflect.ReflectionUtil.isByte;
import static io.higgs.core.reflect.ReflectionUtil.isCollection;
import static io.higgs.core.reflect.ReflectionUtil.isDate;
import static io.higgs.core.reflect.ReflectionUtil.isDouble;
import static io.higgs.core.reflect.ReflectionUtil.isEnum;
import static io.higgs.core.reflect.ReflectionUtil.isFloat;
import static io.higgs.core.reflect.ReflectionUtil.isFractional;
import static io.higgs.core.reflect.ReflectionUtil.isFractionalLike;
import static io.higgs.core.reflect.ReflectionUtil.isInt;
import static io.higgs.core.reflect.ReflectionUtil.isIntLike;
import static io.higgs.core.reflect.ReflectionUtil.isList;
import static io.higgs.core.reflect.ReflectionUtil.isLong;
import static io.higgs.core.reflect.ReflectionUtil.isMap;
import static io.higgs.core.reflect.ReflectionUtil.isNumeric;
import static io.higgs.core.reflect.ReflectionUtil.isQueue;
import static io.higgs.core.reflect.ReflectionUtil.isScalar;
import static io.higgs.core.reflect.ReflectionUtil.isSet;
import static io.higgs.core.reflect.ReflectionUtil.isShort;
import static io.higgs.core.reflect.ReflectionUtil.isString;
import static io.higgs.core.reflect.ReflectionUtil.isStringLike;
import static io.higgs.core.reflect.ReflectionUtil.isUUID;
import static io.higgs.core.reflect.ReflectionUtil.newInstance;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ReflectionUtilTest {
  //make A static, non-static inner classes get a generated constructor which accepts an instance of the enclosing class
  static class A {
    private B a;
    private int b;

    private void m1() {
    }

    protected void m2() {
    }

    public void m3() {
    }
  }

  class B {
    private Long b;
    private String c;

    private int getA() {
      return 0;
    }
  }

  public enum Day { MONDAY }

  @Test
  public void dotPathTest() {
    List<String> list = Arrays.asList("a", "b", "c");
    assertEquals("a.b.c", dotPath(list));
  }

  @Test
  public void dotPathColObjsTest() {
    List<Object> list = Arrays.asList("a", "b", "c", 1);
    assertEquals("a.b.c.1", dotPath(list));
  }

  @Test
  public void fieldsTest() {
    Field b = field("a.b", A.class);
    assertNotNull(b);
    assertEquals("b", b.getName());
    assertEquals(B.class, b.getDeclaringClass());
  }

  @Test
  public void getAllFieldsTest() {
    Map<String, Field> fields = new HashMap<>();
    assertSame(fields, getAllFields(fields, A.class));
    List<Field> list = new ArrayList<>(fields.values());
    list.sort(Comparator.comparing(Field::getName));
    assertEquals("a", list.get(0).getName());
    assertEquals("b", list.get(1).getName());
  }

  @Test
  public void getAllFieldsByName() {
    Map<String, Field> fields = getAllFields(A.class);
    assertNotNull(fields.get("a"));
    assertNotNull(fields.get("b"));
  }

  @Test
  public void getAllMethodsTest() {
    Map<String, Method> methods = getAllMethods(A.class);
    assertNotNull(methods.get("m1"));
    assertNotNull(methods.get("m2"));
    assertNotNull(methods.get("m3"));
  }

  @Test
  public void isNumericTest() {
    assertTrue(isNumeric(int.class));
    assertTrue(isNumeric(Integer.class));
    assertTrue(isNumeric(short.class));
    assertTrue(isNumeric(Short.class));
    assertTrue(isNumeric(byte.class));
    assertTrue(isNumeric(Byte.class));
    assertTrue(isNumeric(long.class));
    assertTrue(isNumeric(Long.class));
    assertTrue(isNumeric(BigInteger.class));
    assertTrue(isNumeric(float.class));
    assertTrue(isNumeric(Float.class));
    assertTrue(isNumeric(double.class));
    assertTrue(isNumeric(Double.class));
    assertTrue(isNumeric(BigDecimal.class));
  }

  @Test
  public void isIntLikeTest() {
    assertTrue(isIntLike(int.class));
    assertTrue(isIntLike(Integer.class));
    assertTrue(isIntLike(long.class));
    assertTrue(isIntLike(Long.class));
    assertTrue(isIntLike(BigInteger.class));
  }

  @Test
  public void isFractionalLikeTest() {
    assertTrue(isFractionalLike(float.class));
    assertTrue(isFractionalLike(Float.class));
    assertTrue(isFractionalLike(double.class));
    assertTrue(isFractionalLike(Double.class));
    assertTrue(isFractionalLike(BigDecimal.class));
  }

  @Test
  public void isBigIntTest() {
    assertTrue(isBigInt(BigInteger.class));
  }

  @Test
  public void isBigDecimalTest() {
    assertTrue(isBigDecimal(BigDecimal.class));
  }

  @Test
  public void isByteTest() {
    assertTrue(isByte(byte.class));
    assertTrue(isByte(Byte.class));
  }

  @Test
  public void isShortTest() {
    assertTrue(isShort(short.class));
    assertTrue(isShort(Short.class));
  }

  @Test
  public void isFractionalTest() {
    assertTrue(isFractional(float.class));
    assertTrue(isFractional(Float.class));
    assertTrue(isFractional(double.class));
    assertTrue(isFractional(Double.class));
  }

  @Test
  public void isFloatTest() {
    assertTrue(isFloat(float.class));
    assertTrue(isFloat(Float.class));
  }

  @Test
  public void isDoubleTest() {
    assertTrue(isDouble(double.class));
    assertTrue(isDouble(Double.class));
  }

  @Test
  public void isLongTest() {
    assertTrue(isLong(long.class));
    assertTrue(isLong(Long.class));
  }

  @Test
  public void isIntTest() {
    assertTrue(isInt(int.class));
    assertTrue(isInt(Integer.class));
  }

  @Test
  public void isStringTest() {
    assertTrue(isString(String.class));
    assertTrue(isStringLike(String.class));
    assertTrue(isStringLike(UUID.class));
    assertTrue(isStringLike(Duration.class));
  }

  @Test
  public void isEnumTest() {
    assertTrue(isEnum(Day.class));
    assertTrue(isEnum(Day.MONDAY.getClass()));
  }

  @Test
  public void isUUIDTest() {
    assertTrue(isUUID(UUID.randomUUID().getClass()));
  }

  @Test
  public void isDateTest() {
    assertTrue(isDate(Date.class));
    assertTrue(isDate(DateTime.class));
    assertTrue(isDate(Temporal.class));
    assertTrue(isDate(java.sql.Date.class));
  }

  @Test
  public void isCollectionTest() {
    assertTrue(isCollection(List.class));
    assertTrue(isCollection(Set.class));
    assertTrue(isCollection(Queue.class));
  }

  @Test
  public void isListTest() {
    assertTrue(isList(List.class));
  }

  @Test
  public void isSetTest() {
    assertTrue(isSet(Set.class));
  }

  @Test
  public void isQueueTest() {
    assertTrue(isQueue(Queue.class));
  }

  @Test
  public void isMapTest() {
    assertTrue(isMap(Map.class));
  }

  @Test
  public void isBoolTest() {
    assertTrue(isBool(boolean.class));
    assertTrue(isBool(Boolean.class));
  }

  @Test
  public void isScalarTest() {
    assertTrue(isScalar(int.class));
    assertTrue(isScalar(boolean.class));
    assertTrue(isScalar(String.class));
    assertTrue(isScalar(Date.class));
    assertTrue(isScalar(DateTime.class));
    assertTrue(isScalar(Day.class));
  }

  @Test
  public void copyTest() {
  }

  @Test
  public void copy1Test() {
  }

  @Test
  public void newInstanceTest() {
    A a = newInstance(A.class);
    assertNotNull(a);
  }

  @Test
  public void getFieldOrGetterTypeTest() {
    assertEquals(B.class, getFieldOrGetterType("a", A.class));
    assertEquals(int.class, getFieldOrGetterType("a", B.class));
  }
}

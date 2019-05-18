package io.higgs.core.reflect;

import org.joda.time.DateTime;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ReflectionUtil {
  public static int MAX_RECURSION_DEPTH = 10;
  public static final ConcurrentMap<String, ConcurrentMap<String, Field>> fieldCache = new ConcurrentHashMap<>();

  private ReflectionUtil() {
  }

  public static String dotPath(Collection<Object> path) {
    return dotPath((List<String>) path.stream().filter(Objects::nonNull).map(Object::toString).collect(toList()));
  }

  public static String dotPath(List<String> path) {
    return path.stream().reduce("", (a, b) -> a.concat(".").concat(b)).substring(1);
  }

  public static Field field(String path, Class<?> root) {
    String[] parts = path.split("\\.");
    Field field = null;
    for (String name : parts) {
      ConcurrentMap<String, Field> fields = fieldCache.getOrDefault(root.getName(), new ConcurrentHashMap<>());
      if (fields.isEmpty()) {
        fields.putAll(getAllFields(root));
      }
      field = fields.get(name);
      if (field == null) {
        return null;
      }
      root = field.getType();
    }
    if (field != null) {
      field.setAccessible(true);
    }
    return field;
  }

  public static ConcurrentMap<String, Field> getAllFields(Class<?> type) {
    Set<Field> all = getAllFields(new HashSet<>(), type, 0);
    ConcurrentHashMap<String, Field> map = new ConcurrentHashMap<>();
    all.forEach(field -> {
      field.setAccessible(true);
      map.put(field.getName(), field);
    });
    return map;
  }

  public static Set<Field> getAllFields(Set<Field> fields, Class<?> type) {
    return getAllFields(fields, type, 0);
  }

  public static Set<Field> getAllFields(Set<Field> fields, Class<?> type, int depth) {
    //first get inherited fields
    if (type.getSuperclass() != null && depth <= MAX_RECURSION_DEPTH) {
      getAllFields(fields, type.getSuperclass(), ++depth);
    }
    //now add all "local" fields
    Collections.addAll(fields, type.getDeclaredFields());
    return fields;
  }

  public static Map<String, Method> getAllMethods(Class<?> klass) {
    Map<String, Method> methods = new HashMap<>();
    getAllMethods(methods, klass);
    return methods;
  }

  public static void getAllMethods(Map<String, Method> methods, Class<?> type) {
    getAllMethods(methods, type, 0);
  }

  public static void getAllMethods(Map<String, Method> methods, Class<?> type, int depth) {
    if (type.getSuperclass() != null && depth <= MAX_RECURSION_DEPTH) {
      getAllMethods(methods, type.getSuperclass(), ++depth);
    }
    for (Method m : type.getDeclaredMethods()) {
      methods.put(m.getName(), m);
    }
  }

  /**
   * @return true if klass represents a numeric type, including byte. Both boxed and unboxed.
   */
  public static boolean isNumeric(Class<?> klass) {
    return isIntLike(klass) ||
      isFractionalLike(klass) ||
      isShort(klass) ||
      isByte(klass);
  }

  public static boolean isIntLike(Class<?> klass) {
    return isInt(klass) || isLong(klass) || isBigInt(klass);
  }

  public static boolean isFractionalLike(Class<?> klass) {
    return isDouble(klass) || isFloat(klass) || isBigDecimal(klass);
  }

  public static boolean isBigInt(Class<?> klass) {
    return BigInteger.class.isAssignableFrom(klass);
  }

  public static boolean isBigDecimal(Class<?> klass) {
    return BigDecimal.class.isAssignableFrom(klass);
  }

  public static boolean isByte(Class<?> klass) {
    return Byte.class.isAssignableFrom(klass) ||
      byte.class.isAssignableFrom(klass);
  }

  public static boolean isShort(Class<?> klass) {
    return Short.class.isAssignableFrom(klass) ||
      short.class.isAssignableFrom(klass);
  }

  public static boolean isFractional(Class<?> klass) {
    return isDouble(klass) ||
      isFloat(klass);
  }

  public static boolean isFloat(Class<?> klass) {
    return Float.class.isAssignableFrom(klass) ||
      float.class.isAssignableFrom(klass);
  }

  public static boolean isDouble(Class<?> klass) {
    return Double.class.isAssignableFrom(klass) ||
      double.class.isAssignableFrom(klass);
  }

  public static boolean isLong(Class<?> klass) {
    return Long.class.isAssignableFrom(klass) ||
      long.class.isAssignableFrom(klass);
  }

  public static boolean isInt(Class<?> klass) {
    return Integer.class.isAssignableFrom(klass) ||
      int.class.isAssignableFrom(klass);
  }

  public static boolean isString(Class<?> cls) {
    return String.class.isAssignableFrom(cls);
  }

  public static boolean isStringLike(Class<?> cls) {
    return isString(cls)
      || isUUID(cls)
      || Duration.class.isAssignableFrom(cls);
  }

  public static boolean isEnum(Class cls) {
    if (cls.isEnum()) {
      return true;
    }
    Class sCls = cls.getSuperclass();
    return sCls != null && sCls.isEnum();
  }

  public static boolean isUUID(Class<?> cls) {
    return UUID.class.isAssignableFrom(cls);
  }

  public static boolean isDate(Class<?> cls) {
    return Date.class.isAssignableFrom(cls)
      || DateTime.class.isAssignableFrom(cls)
      || Temporal.class.isAssignableFrom(cls)
      || java.sql.Date.class.isAssignableFrom(cls);
  }

  public static boolean isCollection(Class<?> cls) {
    return Collection.class.isAssignableFrom(cls);
  }

  public static boolean isList(Class<?> cls) {
    return List.class.isAssignableFrom(cls);
  }

  public static boolean isSet(Class<?> cls) {
    return Set.class.isAssignableFrom(cls);
  }

  public static boolean isQueue(Class<?> cls) {
    return Queue.class.isAssignableFrom(cls);
  }

  public static boolean isMap(Class<?> cls) {
    return Map.class.isAssignableFrom(cls);
  }

  public static boolean isBool(Class cls) {
    return boolean.class.isAssignableFrom(cls) || Boolean.class.isAssignableFrom(cls);
  }

  public static boolean isScalar(Class cls) {
    return isNumeric(cls)
      || isBool(cls)
      || isString(cls)
      || isDate(cls)
      || isEnum(cls);
  }

  @SuppressWarnings("unchecked")
  public static <T> T copy(T o, boolean deep) {
    if (o == null) {
      return null;
    }
    Class<T> cls = (Class<T>) o.getClass();
    return copy(o, deep, null, cls);
  }

  public static <T> T copy(T o, boolean deep, T to, Class<T> cls) {
    try {
      T obj = to == null ? newInstance(cls) : to;
      for (Field field : getAllFields(new LinkedHashSet<>(), cls)) {
        field.setAccessible(true);
        boolean isStatic = Modifier.isStatic(field.getModifiers());
        if (!isStatic) {
          Object fieldVal = field.get(o);
          if (fieldVal != null) {
            if (!deep || isScalar(fieldVal.getClass())) {
              field.set(obj, fieldVal);
            } else {
              if (fieldVal instanceof Set) {
                fieldVal = new HashSet<>((Set) fieldVal);
              } else if (fieldVal instanceof Queue) {
                fieldVal = new LinkedList<>((Queue) fieldVal);
              } else if (fieldVal instanceof Collection) {
                fieldVal = new ArrayList<>((Collection) fieldVal);
              } else if (fieldVal instanceof Map) {
                fieldVal = new HashMap<>((Map) fieldVal);
              } else {
                fieldVal = copy(fieldVal, deep);
              }
              field.set(obj, fieldVal);
            }
          }
        }
      }
      return obj;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create instance", e);
    }
  }

  public static <T> T newInstance(Class<T> cls) {
    Constructor[] ctors = cls.getDeclaredConstructors();
    if (ctors.length == 0) {
      throw new UnsupportedOperationException("Cannot create object if it is an interface, a primitive type, " +
        "an array class, or void");
    }
    T obj = null;
    for (Constructor ctor : ctors) {
      if (ctor.getParameterCount() == 0) {
        ctor.setAccessible(true);
        try {
          obj = (T) ctor.newInstance();
        } catch (InstantiationException | IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
          throw new IllegalStateException(format("Unable to create instance of %s", cls.getName()), e);
        }
        break;
      }
    }
    if (obj == null) {
      throw new UnsupportedOperationException(format("%s cannot be created, no-arg constructor not found",
        cls.getName()));
    }
    return obj;
  }

  public static Class<?> getFieldOrGetterType(String name, Class<?> cls) {
    for (Field field : getAllFields(new HashSet<>(), cls)) {
      if (field.getName().contentEquals(name)) {
        return field.getType();
      }
    }
    String inName = format("get%s", name);
    HashMap<String, Method> methods = new HashMap<>();
    getAllMethods(methods, cls);
    for (Method method : methods.values()) {
      String methodName = method.getName();
      if (methodName.toLowerCase().contentEquals(inName)) {
        return method.getReturnType();
      }
    }
    return null;
  }
}

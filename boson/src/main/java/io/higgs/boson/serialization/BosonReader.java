package io.higgs.boson.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.higgs.core.reflect.ReflectionUtil;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.higgs.boson.BosonType.ARRAY;
import static io.higgs.boson.BosonType.BOOLEAN;
import static io.higgs.boson.BosonType.BYTE;
import static io.higgs.boson.BosonType.BYTE_ARRAY;
import static io.higgs.boson.BosonType.CHAR;
import static io.higgs.boson.BosonType.DATE;
import static io.higgs.boson.BosonType.DOUBLE;
import static io.higgs.boson.BosonType.DURATION;
import static io.higgs.boson.BosonType.ENUM;
import static io.higgs.boson.BosonType.FLOAT;
import static io.higgs.boson.BosonType.INT;
import static io.higgs.boson.BosonType.JODA_DATETIME;
import static io.higgs.boson.BosonType.JODA_DURATION;
import static io.higgs.boson.BosonType.JODA_INTERVAL;
import static io.higgs.boson.BosonType.JODA_LOCALTIME;
import static io.higgs.boson.BosonType.JODA_LOCAL_DATE;
import static io.higgs.boson.BosonType.JODA_LOCAL_DATE_TIME;
import static io.higgs.boson.BosonType.JODA_PERIOD;
import static io.higgs.boson.BosonType.LIST;
import static io.higgs.boson.BosonType.LOCALTIME;
import static io.higgs.boson.BosonType.LOCAL_DATE;
import static io.higgs.boson.BosonType.LOCAL_DATETIME;
import static io.higgs.boson.BosonType.LONG;
import static io.higgs.boson.BosonType.MAP;
import static io.higgs.boson.BosonType.NULL;
import static io.higgs.boson.BosonType.PERIOD;
import static io.higgs.boson.BosonType.POLO;
import static io.higgs.boson.BosonType.REFERENCE;
import static io.higgs.boson.BosonType.SET;
import static io.higgs.boson.BosonType.SHORT;
import static io.higgs.boson.BosonType.STRING;
import static io.higgs.core.reflect.ReflectionUtil.classOf;
import static io.higgs.core.reflect.ReflectionUtil.getAllFields;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A reader for data generated by {@link BosonWriter}
 */
public class BosonReader {
  private static final String invalidMsgStr = "BosonReader tried to read additional data from an unreadable buffer. " +
                                                "Possible data corruption.";
  private static final Logger log = LoggerFactory.getLogger(BosonReader.class);
  private static final BosonReader instance = new BosonReader();

  public static class ReaderCtx {
    public boolean readPoloAsMap;
    byte version = BosonWriter.WriterCtx.version;
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private IdentityHashMap<Integer, Object> references = new IdentityHashMap<>();
    private ObjectMapper mapper;
    private DataInput buf;
    //private Enhancer enhancer = new Enhancer();

    public ReaderCtx() {
      mapper = new ObjectMapper();
    }

    public ReaderCtx(final ObjectMapper mapper) {
      this.mapper = mapper;
    }
  }

  protected BosonReader() {
  }

  public static BosonReader getInstance() {
    return instance;
  }

  public static <T> T decode(byte[] data) {
    return decode(data, new ReaderCtx());
  }

  public static <T> T decode(byte[] data, ReaderCtx ctx) {
    ctx.buf = new DataInputStream(new ByteArrayInputStream(data));
    return decode(ctx);
  }

  public static <T> T decode(DataInput input) {
    ReaderCtx ctx = new ReaderCtx();
    ctx.buf = input;
    return decode(ctx);
  }

  public static <T> T decode(ReaderCtx ctx) {
    return decode(ctx, instance);
  }

  public static <T> T decode(ReaderCtx ctx, BosonReader reader) {
    try {
      byte dataVersion = ctx.buf.readByte();
      if (ctx.version != dataVersion) {
        throw new UnsupportedEncodingException(format(
          "Data version %s is not compatible with this reader which can only ready version %s of boson data",
          dataVersion, ctx.version
        ));
      }
      Object obj = reader.readType(ctx);
      return (T) obj;
    } catch (Exception ioe) {
      throw new InvalidDataException(invalidMsgStr, ioe);
    }
  }

  /**
   * Read a UTF-8 string from the buffer
   *
   * @param ctx          the ctx
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the string
   */
  private String readString(ReaderCtx ctx, boolean verified, byte verifiedType) throws Exception {
    byte type = verifiedType;
    if (!verified) {
      type = ctx.buf.readByte();
    }
    if (STRING.id == type) {
      //read size of type - how many bytes are in the string
      int size = ctx.buf.readInt();
      if (size == 0) {
        return "";
      }
      //read type's payload and de-serialize
      byte[] bytes = new byte[size];
      ctx.buf.readFully(bytes, 0, size);
      return new String(bytes, UTF_8);
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson STRING", type), null);
    }
  }

  private Enum<?> readEnum(ReaderCtx ctx) throws Exception {
    String enumClassName = readString(ctx, false, (byte) 0);
    String enumValue = readString(ctx, false, (byte) 0);

    Class<?> klass;
    try {
      klass = ctx.loader.loadClass(enumClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(format(
        "Cannot load the requested class %s",
        enumClassName
      ), e);
    }
    Enum<?>[] vals = (Enum<?>[]) klass.getEnumConstants();
    if (vals != null && vals.length > 0) {
      for (Enum<?> e : vals) {
        if (e.toString().equals(enumValue)) {
          return e;
        }
      }
    }
    //TODO review what happens if enum type not found when de-serializing
    return null;
  }

  /**
   * Read a single byte from the buffer
   *
   * @return the byte
   */
  private byte readByte(ReaderCtx ctx) throws Exception {
    return ctx.buf.readByte();
  }

  /**
   * Read a short (16 bits) from the buffer
   *
   * @return the short
   */
  private short readShort(ReaderCtx ctx) throws Exception {
    return ctx.buf.readShort();
  }

  /**
   * Read an int (4 bytes) from the buffer
   *
   * @return the int
   */
  private int readInt(ReaderCtx ctx) throws Exception {
    return ctx.buf.readInt();
  }

  /**
   * Read a long (8 bytes) from the buffer
   *
   * @return the long
   */
  private long readLong(ReaderCtx ctx) throws Exception {
    return ctx.buf.readLong();
  }

  /**
   * Read a float (32 bit floating point) from the buffer
   *
   * @return the float
   */
  private float readFloat(ReaderCtx ctx) throws Exception {
    return ctx.buf.readFloat();
  }

  /**
   * Read a double (64 bit floating point) from the buffer
   *
   * @return the double
   */
  private double readDouble(ReaderCtx ctx) throws Exception {
    return ctx.buf.readDouble();
  }

  /**
   * Read a a single byte from the buffer   if the byte is 1 then returns true, otherwise false
   *
   * @return the boolean
   */
  private boolean readBoolean(ReaderCtx ctx) throws Exception {
    return ctx.buf.readByte() != 0;
  }

  /**
   * Read a char (16 bits) from the buffer
   *
   * @return the char
   */
  private char readChar(ReaderCtx ctx) throws Exception {
    return ctx.buf.readChar();
  }

  /**
   * Read an array from the buffer
   *
   * @param ctx the ctx
   * @return the array
   */
  private Object readArray(ReaderCtx ctx) throws Exception {
    //read number of elements in the array
    int size = ctx.buf.readInt();
    String componentTypeName = readString(ctx, false, (byte) 0);
    Class<?> componentType = classOf(ctx.loader, componentTypeName);
    Object arr = null;
    for (int i = 0; i < size; i++) {
      final byte type = ctx.buf.readByte();
      Object obj = readType(ctx, type);
      if (obj != null && arr == null) {
        arr = Array.newInstance(componentType, size);
      }
      Array.set(arr, i, obj);
    }
    return arr;
  }

  private byte[] readByteArray(ReaderCtx ctx) throws Exception {
    //read number of elements in the array
    int size = ctx.buf.readInt();
    byte[] arr = new byte[size];
    ctx.buf.readFully(arr, 0, size);
    return arr;
  }

  /**
   * Read a List from the buffer
   *
   * @return the list
   */
  private List<Object> readList(ReaderCtx ctx) throws Exception {
    //read number of elements in the array
    int size = ctx.buf.readInt();
    List<Object> arr = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      //get type of this element in the array
      final byte type = ctx.buf.readByte();
      //at this stage only basic data types are allowed
      arr.add(readType(ctx, type));
    }
    return arr;
  }

  private Set<Object> readSet(ReaderCtx ctx) throws Exception {
    //read number of elements in the array
    int size = ctx.buf.readInt();
    Set<Object> set = new HashSet<>();
    for (int i = 0; i < size; i++) {
      //get type of this element in the array
      byte type = ctx.buf.readByte();
      //at this stage only basic data types are allowed
      set.add(readType(ctx, type));
    }
    return set;
  }

  /**
   * Read a map (list of key -> value pairs) from the buffer
   *
   * @return the map
   */
  private Map<Object, Object> readMap(ReaderCtx ctx) throws Exception {
    int size = ctx.buf.readInt();
    Map<Object, Object> kv = new HashMap<>();
    for (int i = 0; i < size; i++) {
      byte keyType = ctx.buf.readByte();
      Object key = readType(ctx, keyType);
      byte valueType = ctx.buf.readByte();
      Object value = readType(ctx, valueType);
      kv.put(key, value);
    }
    return kv;
  }

  private Object readPolo(ReaderCtx ctx) throws Exception {
    //get reference
    int ref = ctx.buf.readInt();
    //get class name
    String poloClassName = readString(ctx, false, (byte) 0);
    if (poloClassName.isEmpty()) {
      throw new InvalidDataException("Cannot de-serialise a POLO without it's fully qualified class name " +
                                       "being provided", null);
    }
    //get number of fields serialized
    int size = ctx.buf.readInt();
    if (ctx.readPoloAsMap) {
      return readPoloMap(ctx, ref, size);
    } else {
      boolean isJsonArray = poloClassName.contentEquals(ArrayNode.class.getName());
      boolean isJsonObject = poloClassName.contentEquals(ObjectNode.class.getName());
      if (isJsonArray || isJsonObject) {
        return readJson(ctx, isJsonArray, ref, size);
      } else {
        return readPoloReflection(ctx, poloClassName, ref, size);
      }
    }
  }

  private Object readJson(ReaderCtx ctx, boolean isArray, int ref, int size) throws Exception {
    JsonNode instance = isArray ? ctx.mapper.createArrayNode() : ctx.mapper.createObjectNode();
    ctx.references.put(ref, instance);
    for (int i = 0; i < size; i++) {
      //polo keys are required to be strings
      String key = readString(ctx, false, (byte) 0);
      byte valueType = ctx.buf.readByte();
      Object value = readType(ctx, valueType);
      JsonNode json = readJsonType(value);
      if (isArray) {
        ((ArrayNode) instance).add(json);
      } else {
        ((ObjectNode) instance).set(key, json);
      }
    }
    return instance;
  }

  private Object readPoloMap(ReaderCtx ctx, int ref, int size) throws Exception {
    Map<String, Object> instance = new HashMap<>();
    //Put the instance in the reference table
    ctx.references.put(ref, instance);
    for (int i = 0; i < size; i++) {
      //polo keys are required to be strings
      String key = readString(ctx, false, (byte) 0);
      byte valueType = ctx.buf.readByte();
      Object value = readType(ctx, valueType);
      instance.put(key, value);
    }
    return instance;
  }

  private Object readPoloReflection(ReaderCtx ctx, String poloClassName, int ref, int size) throws Exception {
    //try to load the class if available
    Class<?> klass;
    try {
      klass = ctx.loader.loadClass(poloClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(format("Cannot load the requested class %s", poloClassName), e);
    }
    Object instance = ReflectionUtil.newInstance(klass);
    //Put the instance in the reference table
    ctx.references.put(ref, instance);
    //create a map of fields names -> Field
    Map<String, Field> fieldset = getAllFields(klass);
    for (int i = 0; i < size; i++) {
      //polo keys are required to be strings
      String key = readString(ctx, false, (byte) 0);
      byte valueType = ctx.buf.readByte();
      Object value = readType(ctx, valueType);
      Field field = fieldset.get(key);
      if (field != null && value != null) {
        Class<?> valueCls = value.getClass();
        //if field's type is an array  create an array of it's type
        Class<?> fieldType = field.getType();
        String cname = valueCls.getName();
        if (fieldType.isArray()) {
          if (valueCls.isArray()) {
            int length = Array.getLength(value);
            //create an array of the expected type
            Object arr = Array.newInstance(fieldType.getComponentType(), length);
            for (int j = 0; j < length; j++) {
              try {
                //get current array value
                Object arrayValue = Array.get(value, j);
                Array.set(arr, j, arrayValue); //set the value at the current index, i
              } catch (IllegalArgumentException iae) {
                log.warn(format("Field \":%s\" of class \"%s\" is an array but " +
                                  "failed to set value at index \"%s\" - type \"%s\"",
                  key, klass.getName(), j, cname
                ));
              }
            }
            try {
              field.set(instance, arr);
            } catch (IllegalAccessException e) {
              log.debug(format("Unable to access field \"%s\" of class \"%s\" ", key,
                klass.getName()
              ));
            }
          } else {
            log.warn(format("Field \":%s\" of class \"%s\" is an array but value " +
                              "received is \"%s\" of type \"%s\"", key, klass.getName(), value, cname));
          }
        } else {
          try {
            field.set(instance, value);
          } catch (IllegalArgumentException iae) {
            String vclass = valueCls.getName();
            log.warn(format("Field \"%s\" of class \"%s\" is of type %s " +
                              "but value received is \"%s\" of type \"%s\"",
              key, klass.getName(), vclass, value, cname
            ));
          } catch (IllegalAccessException e) {
            log.debug(format("Unable to access field \"%s\" of class \"%s\" ",
              key, klass.getName()
            ));
          }
        }
      } else {
        if (value != null) {
          log.warn(format("Field %s received with value %s but the " +
                            "field does not exist in class %s", key, value, poloClassName));
        }
      }
    }
    return instance;
  }

  private Object readReference(ReaderCtx ctx, byte verifiedType) throws Exception {
    Object obj;
    if (REFERENCE.id == verifiedType) {
      int reference = ctx.buf.readInt();
      obj = ctx.references.get(reference);
      return obj;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson reference", verifiedType), null);
    }
  }

  /**
   * Read the next type from the buffer.
   * The type param must match one of Boson's supported types otherwise an exception is thrown
   *
   * @param type the 1 byte integer representing a Boson type
   * @return the type
   */
  private Object readType(ReaderCtx ctx, byte type) throws Exception {
    if (type == BYTE.id) {
      return readByte(ctx);
    } else if (type == SHORT.id) {
      return readShort(ctx);
    } else if (type == INT.id) {
      return readInt(ctx);
    } else if (type == LONG.id) {
      return readLong(ctx);
    } else if (type == FLOAT.id) {
      return readFloat(ctx);
    } else if (type == DOUBLE.id) {
      return readDouble(ctx);
    } else if (type == BOOLEAN.id) {
      return readBoolean(ctx);
    } else if (type == CHAR.id) {
      return readChar(ctx);
    } else if (type == NULL.id) {
      return null;
    } else if (type == STRING.id) {
      return readString(ctx, true, type);
    } else if (type == ARRAY.id) {
      return readArray(ctx);
    } else if (type == BYTE_ARRAY.id) {
      return readByteArray(ctx);
    } else if (type == LIST.id) {
      return readList(ctx);
    } else if (type == SET.id) {
      return readSet(ctx);
    } else if (type == MAP.id) {
      return readMap(ctx);
    } else if (type == POLO.id) {
      return readPolo(ctx);
    } else if (type == REFERENCE.id) {
      return readReference(ctx, type);
    } else if (type == ENUM.id) {
      return readEnum(ctx);
    } else if (type == DATE.id) {
      return new Date(ctx.buf.readLong());
    } else if (type == LOCAL_DATE.id) {
      return LocalDate.ofEpochDay(ctx.buf.readLong());
    } else if (type == LOCAL_DATETIME.id) {
      return LocalDateTime.parse(readString(ctx, false, (byte) 0));
    } else if (type == LOCALTIME.id) {
      return LocalTime.parse(readString(ctx, false, (byte) 0));
    } else if (type == DURATION.id) { //ISO-8601 seconds based representation, such as PT8H6M12.345S.
      //see https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#toString--
      return Duration.parse(readString(ctx, false, (byte) 0));
    } else if (type == PERIOD.id) { //Outputs this period as a String, such as P6Y3M1D.
      //see https://docs.oracle.com/javase/8/docs/api/java/time/Period.html#toString--
      return Period.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_DATETIME.id) {
      return new DateTime(ctx.buf.readLong());
    } else if (type == JODA_LOCAL_DATE.id) {
      return org.joda.time.LocalDate.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_LOCAL_DATE_TIME.id) {
      return org.joda.time.LocalDateTime.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_LOCALTIME.id) {
      return org.joda.time.LocalTime.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_DURATION.id) {
      return org.joda.time.Duration.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_INTERVAL.id) {
      return Interval.parse(readString(ctx, false, (byte) 0));
    } else if (type == JODA_PERIOD.id) {
      return org.joda.time.Period.parse(readString(ctx, false, (byte) 0));
    }
    throw new UnsupportedBosonTypeException(format("type %s is not a valid boson type", type), null);
  }

  public JsonNode readJsonType(Object data) {
    if (data == null) {
      return NullNode.getInstance();
    } else if (data instanceof JsonNode) {
      return (JsonNode) data;
    } else if (data instanceof String) {
      return TextNode.valueOf((String) data);
    } else if (data instanceof Short) {
      return ShortNode.valueOf((Short) data);
    } else if (data instanceof Integer) {
      return IntNode.valueOf((Integer) data);
    } else if (data instanceof Long) {
      return LongNode.valueOf((Long) data);
    } else if (data instanceof Double) {
      return DoubleNode.valueOf((Double) data);
    } else if (data instanceof Float) {
      return FloatNode.valueOf((Float) data);
    } else if (data instanceof Boolean) {
      return BooleanNode.valueOf((Boolean) data);
    } else if (data instanceof byte[]) {
      return BinaryNode.valueOf((byte[]) data);
    } else {
      throw new IllegalStateException(format("%s cannot be read to a JSON type", data.getClass().getName()));
    }
  }

  private Object readType(ReaderCtx ctx) throws Exception {
    return readType(ctx, ctx.buf.readByte());
  }

}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
import static io.higgs.boson.BosonType.DOUBLE;
import static io.higgs.boson.BosonType.ENUM;
import static io.higgs.boson.BosonType.FLOAT;
import static io.higgs.boson.BosonType.INT;
import static io.higgs.boson.BosonType.LIST;
import static io.higgs.boson.BosonType.LONG;
import static io.higgs.boson.BosonType.MAP;
import static io.higgs.boson.BosonType.NULL;
import static io.higgs.boson.BosonType.POLO;
import static io.higgs.boson.BosonType.REFERENCE;
import static io.higgs.boson.BosonType.SET;
import static io.higgs.boson.BosonType.SHORT;
import static io.higgs.boson.BosonType.STRING;
import static java.lang.String.format;

/**
 * @author Courtney Robinson <courtney@crlog.info>
 */
public class BosonReader {
  private static String invalidMsgStr = "BosonReader tried to read additional data from an unreadable buffer. " +
                                          "Possible data corruption.";
  private Logger log = LoggerFactory.getLogger(getClass());
  private ClassLoader loader = Thread.currentThread().getContextClassLoader();
  private IdentityHashMap<Integer, Object> references = new IdentityHashMap<>();
  private ObjectMapper mapper = new ObjectMapper();

  public BosonReader() {
  }

  public BosonReader(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public <T> T deSerialise(byte[] data) {
    DataInputStream buf = new DataInputStream(new ByteArrayInputStream(data));
    return deSerialise(buf);
  }

  public <T> T deSerialise(DataInputStream buf) {
    try {
      Object obj = readType(buf);
      return (T) obj;
    } catch (IOException ioe) {
      throw new InvalidDataException(invalidMsgStr, ioe);
    }
  }

  /**
   * Check that the backing buffer is readable.
   * If it isn't throws an InvalidDataException
   *
   * @throws InvalidDataException if buffer is not readable
   */
  private void verifyReadable(DataInputStream data) {
    try {
      if (data.available() == 0) {
        throw new InvalidDataException(invalidMsgStr, null);
      }
    } catch (IOException ioe) {
      throw new InvalidDataException(invalidMsgStr, ioe);
    }
  }

  /**
   * Read a UTF-8 string from the buffer
   *
   * @param data
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the string
   */
  private String readString(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (STRING == type) {
      verifyReadable(data);
      //read size of type - how many bytes are in the string
      int size = data.readInt();
      if (size == 0) {
        return "";
      }
      //read type's payload and de-serialize
      byte[] bytes = new byte[size];
      data.read(bytes);
      return new String(bytes, Charset.forName("utf8"));
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson STRING", type), null);
    }
  }

  private Enum readEnum(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (ENUM == type) {
      verifyReadable(data);
      String enumClassName = readString(data, false, -1);
      String enumValue = readString(data, false, -1);

      Class<?> klass;
      try {
        klass = loader.loadClass(enumClassName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(format("Cannot load the requested class %s",
          enumClassName), e);
      }
      Enum[] vals = (Enum[]) klass.getEnumConstants();
      if (vals != null && vals.length > 0) {
        for (Enum e : vals) {
          if (e.toString().equals(enumValue)) {
            return e;
          }
        }
      }
      //TODO review what happens if enum type not found when de-serializing
      return null;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson ENUM", type), null);
    }
  }

  /**
   * Read a single byte from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the byte
   */
  private byte readByte(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (BYTE == type) {
      verifyReadable(data);
      return data.readByte();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson BYTE", type), null);
    }
  }

  /**
   * Read a short (16 bits) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the short
   */
  private short readShort(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (SHORT == type) {
      verifyReadable(data);
      return data.readShort();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson SHORT", type), null);
    }
  }

  /**
   * Read an int (4 bytes) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the int
   */
  private int readInt(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (INT == type) {
      verifyReadable(data);
      return data.readInt();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson INT", type), null);
    }
  }

  /**
   * Read a long (8 bytes) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the long
   */
  private long readLong(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (LONG == type) {
      verifyReadable(data);
      return data.readLong();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson LONG", type), null);
    }
  }

  /**
   * Read a float (32 bit floating point) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the float
   */
  private float readFloat(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (FLOAT == type) {
      verifyReadable(data);
      return data.readFloat();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson FLOAT", type), null);
    }
  }

  /**
   * Read a double (64 bit floating point) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the double
   */
  private double readDouble(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (DOUBLE == type) {
      verifyReadable(data);
      return data.readDouble();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson DOUBLE", type), null);
    }
  }

  /**
   * Read a a single byte from the buffer   if the byte is 1 then returns true, otherwise false
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the boolean
   */
  private boolean readBoolean(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (BOOLEAN == type) {
      verifyReadable(data);
      return data.readByte() != 0;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson BOOLEAN", type), null);
    }
  }

  /**
   * Read a char (16 bits) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the char
   */
  private char readChar(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (CHAR == type) {
      verifyReadable(data);
      return data.readChar();
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson CHAR", type), null);
    }
  }

  /**
   * Read an array from the buffer
   *
   * @param data
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the array
   */
  private Object[] readArray(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (ARRAY == type) {
      //read number of elements in the array
      int size = data.readInt();
      Object[] arr = new Object[size];
      for (int i = 0; i < size; i++) {
        type = data.readByte();
        arr[i] = readType(data, type);
      }
      return arr;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson ARRAY", type), null);
    }
  }

  private byte[] readByteArray(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (BYTE_ARRAY == type) {
      //read number of elements in the array
      int size = data.readInt();
      byte[] arr = new byte[size];
      data.read(arr);
      return arr;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson ARRAY", type), null);
    }
  }

  /**
   * Read a List from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the list
   */
  private List<Object> readList(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (LIST == type) {
      //read number of elements in the array
      int size = data.readInt();
      List<Object> arr = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        verifyReadable(data);
        //get type of this element in the array
        type = data.readByte();
        //at this stage only basic data types are allowed
        arr.add(readType(data, type));
      }
      return arr;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson LIST", type), null);
    }
  }

  private Set<Object> readSet(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (SET == type) {
      //read number of elements in the array
      int size = data.readInt();
      Set<Object> set = new HashSet<>();
      for (int i = 0; i < size; i++) {
        verifyReadable(data);
        //get type of this element in the array
        type = data.readByte();
        //at this stage only basic data types are allowed
        set.add(readType(data, type));
      }
      return set;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson SET", type), null);
    }
  }

  /**
   * Read a map (list of key -> value pairs) from the buffer
   *
   * @param verified     if true then the verifiedType param is used to match the type, if false then
   *                     a single byte is read from the buffer to determine the type
   * @param verifiedType the data type to be de-serialized
   * @return the map
   */
  private Map<Object, Object> readMap(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (MAP == type) {
      int size = data.readInt();
      Map<Object, Object> kv = new HashMap<>();
      for (int i = 0; i < size; i++) {
        verifyReadable(data);
        int keyType = data.readByte();
        Object key = readType(data, keyType);
        int valueType = data.readByte();
        Object value = readType(data, valueType);
        kv.put(key, value);
      }
      return kv;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson MAP", type), null);
    }
  }

  private Object readPolo(DataInputStream data, boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    if (POLO == type) {
      verifyReadable(data);
      //get reference
      int ref = data.readInt();
      //get class name
      String poloClassName = readString(data, false, -1);
      if (poloClassName == null || poloClassName.isEmpty()) {
        throw new InvalidDataException("Cannot de-serialise a POLO without it's fully qualified class name " +
                                         "being provided", null);
      }
      boolean isJsonArray = poloClassName.contentEquals(ArrayNode.class.getName());
      boolean isJsonObject = poloClassName.contentEquals(ObjectNode.class.getName());
      //get number of fields serialized
      int size = data.readInt();
      if (isJsonArray || isJsonObject) {
        return readJson(data, isJsonArray, ref, size);
      } else {
        return readPoloReflection(data, poloClassName, ref, size);
      }
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson POLO", type), null);
    }
  }

  private Object readJson(DataInputStream data, boolean isArray, int ref, int size) throws IOException {
    JsonNode instance = isArray ? mapper.createArrayNode() : mapper.createObjectNode();
    references.put(ref, instance);
    for (int i = 0; i < size; i++) {
      verifyReadable(data);
      //polo keys are required to be strings
      String key = readString(data, false, 0);
      verifyReadable(data);
      int valueType = data.readByte();
      Object value = readType(data, valueType);
      JsonNode json = readJsonType(value);
      if (isArray) {
        ((ArrayNode) instance).add(json);
      } else {
        ((ObjectNode) instance).set(key, json);
      }
    }
    return instance;
  }

  private Object readPoloReflection(DataInputStream data, String poloClassName, int ref, int size) throws IOException {
    //try to load the class if available
    try {
      Class<?> klass;
      try {
        klass = loader.loadClass(poloClassName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(format("Cannot load the requested class %s",
          poloClassName), e);
      }
      Object instance = klass.newInstance();
      //Put the instance in the reference table
      references.put(ref, instance);
      //get ALL (public,private,protect,package) fields declared in the class - excludes inherited fields
      Set<Field> fields = ReflectionUtil.getAllFields(new HashSet<Field>(), klass, 0);
      //create a map of fields names -> Field
      Map<String, Field> fieldset = new HashMap<>();
      for (Field field : fields) {
        if (!Modifier.isFinal(field.getModifiers()) &&
              !Modifier.isTransient(field.getModifiers())) {
          //only add non-final and transient fields
          fieldset.put(field.getName(), field);
        }
      }
      for (int i = 0; i < size; i++) {
        verifyReadable(data);
        //polo keys are required to be strings
        String key = readString(data, false, 0);
        verifyReadable(data);
        int valueType = data.readByte();
        Object value = readType(data, valueType);
        Field field = fieldset.get(key);
        if (field != null && value != null) {
          field.setAccessible(true);
          //if field's type is an array  create an array of it's type
          Class<?> fieldType = field.getType();
          String cname = value == null ? "null" : value.getClass().getName();
          if (fieldType.isArray()) {
            if (value.getClass().isArray()) {
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
                  klass.getName()));
              }
            } else {
              log.warn(format("Field \":%s\" of class \"%s\" is an array but value " +
                                "received is \"%s\" of type \"%s\"", key, klass.getName(), value, cname));
            }
          } else {
            try {
              field.set(instance, value);
            } catch (IllegalArgumentException iae) {
              String vclass = value.getClass().getName();
              log.warn(format("Field \"%s\" of class \"%s\" is of type %s " +
                                "but value received is \"%s\" of type \"%s\"",
                key, klass.getName(), vclass, value, cname
              ));
            } catch (IllegalAccessException e) {
              log.debug(format("Unable to access field \"%s\" of class \"%s\" ",
                key, klass.getName()));
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
    } catch (InstantiationException e) {
      log.warn("Unable to create an instance", e);
    } catch (IllegalAccessException e) {
      log.debug("Unable to access field", e);
    }
    return null;
  }

  private Object readReference(DataInputStream data, final boolean verified, int verifiedType) throws IOException {
    int type = verifiedType;
    if (!verified) {
      type = data.readByte();
    }
    Object obj;
    if (REFERENCE == type) {
      int reference = data.readInt();
      obj = references.get(reference);
      return obj;
    } else {
      throw new UnsupportedBosonTypeException(format("type %s is not a Boson reference", type), null);
    }
  }

  /**
   * Read the next type from the buffer.
   * The type param must match one of Boson's supported types otherwise an exception is thrown
   *
   * @param type the 1 byte integer representing a Boson type
   * @return the type
   */
  private Object readType(DataInputStream data, int type) throws IOException {
    switch (type) {
      case BYTE:
        return readByte(data, true, type);
      case SHORT:
        return readShort(data, true, type);
      case INT:
        return readInt(data, true, type);
      case LONG:
        return readLong(data, true, type);
      case FLOAT:
        return readFloat(data, true, type);
      case DOUBLE:
        return readDouble(data, true, type);
      case BOOLEAN:
        return readBoolean(data, true, type);
      case CHAR:
        return readChar(data, true, type);
      case NULL:
        return null;
      case STRING:
        return readString(data, true, type);
      case ARRAY:
        return readArray(data, true, type);
      case BYTE_ARRAY:
        return readByteArray(data, true, type);
      case LIST:
        return readList(data, true, type);
      case SET:
        return readSet(data, true, type);
      case MAP:
        return readMap(data, true, type);
      case POLO:
        return readPolo(data, true, type);
      case REFERENCE:
        return readReference(data, true, type);
      case ENUM:
        return readEnum(data, true, ENUM);
      default: {
        throw new UnsupportedBosonTypeException(format("type %s is not a valid boson type", type), null);
      }
    }
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

  private Object readType(DataInputStream buffer) throws IOException {
    return readType(buffer, buffer.readByte());
  }

}

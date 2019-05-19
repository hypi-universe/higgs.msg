package io.higgs.boson.serialization;

import com.fasterxml.jackson.databind.JsonNode;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
 * The Boson object serialiser
 */
public class BosonWriter {
  private static final Charset utf8 = Charset.forName("utf-8");
  protected final HashMap<Integer, Integer> references = new HashMap<>();
  protected final AtomicInteger reference = new AtomicInteger();
  private Logger log = LoggerFactory.getLogger(getClass());
  private final int version = 1;
  private boolean serialiseFinalFields;

  public BosonWriter(boolean serialiseFinalFields) {
    this.serialiseFinalFields = serialiseFinalFields;
  }

  public BosonWriter() {
  }

  /**
   * Serialize any object to a series of bytes.
   *
   * @param msg the message to serialize
   * @return a series of bytes representing the message
   */
  public byte[] serialize(Object msg) {
    ByteArrayOutputStream arr = new ByteArrayOutputStream();
    DataOutputStream buffer = new DataOutputStream(arr);
    try {
      buffer.writeByte(version);
      validateAndWriteType(buffer, msg);
    } catch (IOException ioe) {
      throw new InvalidDataException("Serialisation error", ioe);
    }
    return arr.toByteArray(); //not sure there's a way to avoid the memory copy here
  }

  private void writeByte(DataOutputStream buffer, byte b) throws IOException {
    buffer.writeByte(BYTE);
    buffer.writeByte(b);
  }

  private void writeNull(DataOutputStream buffer) throws IOException {
    buffer.writeByte(NULL);
  }

  private void writeShort(DataOutputStream buffer, short s) throws IOException {
    buffer.writeByte(SHORT);
    buffer.writeShort(s);
  }

  private void writeInt(DataOutputStream buffer, int i) throws IOException {
    buffer.writeByte(INT);
    buffer.writeInt(i);
  }

  private void writeLong(DataOutputStream buffer, long l) throws IOException {
    buffer.writeByte(LONG);
    buffer.writeLong(l);
  }

  private void writeFloat(DataOutputStream buffer, float f) throws IOException {
    buffer.writeByte(FLOAT);
    buffer.writeFloat(f);
  }

  private void writeDouble(DataOutputStream buffer, double d) throws IOException {
    buffer.writeByte(DOUBLE);
    buffer.writeDouble(d);
  }

  private void writeBoolean(DataOutputStream buffer, boolean b) throws IOException {
    buffer.writeByte(BOOLEAN);
    if (b) {
      buffer.writeByte(1);
    } else {
      buffer.writeByte(0);
    }
  }

  private void writeChar(DataOutputStream buffer, char c) throws IOException {
    buffer.writeByte(CHAR);
    buffer.writeChar(c);
  }

  private void writeString(DataOutputStream buffer, String s) throws IOException {
    buffer.writeByte(STRING); //type
    byte[] str = s.getBytes(utf8);
    buffer.writeInt(str.length); //size
    buffer.write(str); //payload
  }

  private void writeEnum(DataOutputStream buf, Enum param) throws IOException {
    buf.writeByte(ENUM); //type
    writeString(buf, param.getClass().getName()); //enum class type
    writeString(buf, param.toString()); //enum value
  }

  private void writeList(DataOutputStream buffer, Iterator value, int size) throws IOException {
    buffer.writeByte(LIST); //type
    buffer.writeInt(size); //size
    while (value.hasNext()) {
      Object param = value.next();
      if (param == null) {
        writeNull(buffer);
      } else {
        validateAndWriteType(buffer, param); //payload
      }
    }
  }

  private void writeSet(DataOutputStream buffer, Set<Object> value) throws IOException {
    buffer.writeByte(SET); //type
    buffer.writeInt(value.size()); //size
    for (Object param : value) {
      if (param == null) {
        writeNull(buffer);
      } else {
        validateAndWriteType(buffer, param); //payload
      }
    }
  }

  /**
   * Write an array of any supported boson type to the given buffer.
   * If the buffer contains any unsupported type, this will fail by throwing an UnsupportedBosonTypeException
   *
   * @param value the value to write
   */
  private void writeArray(DataOutputStream buffer, Object value) throws IOException {
    buffer.writeByte(ARRAY); //type
    int length = Array.getLength(value);
    buffer.writeInt(length); //size
    writeString(buffer, value.getClass().getComponentType().getName()); //component type
    for (int i = 0; i < length; i++) {
      validateAndWriteType(buffer, Array.get(value, i)); //payload
    }
  }

  private void writeByteArray(DataOutputStream buffer, byte[] value) throws IOException {
    buffer.writeByte(BYTE_ARRAY); //type
    buffer.writeInt(value.length); //size
    buffer.write(value); //payload
  }

  private void writeMap(DataOutputStream buffer, Map<?, ?> value) throws IOException {
    buffer.writeByte(MAP); //type
    buffer.writeInt(value.size()); //size
    for (Object key : value.keySet()) {
      Object v = value.get(key);
      validateAndWriteType(buffer, key); //key payload
      validateAndWriteType(buffer, v); //value payload
    }
  }

  /**
   * Serialize any* Java object.
   * Circular reference support based on
   * http://beza1e1.tuxen.de/articles/deepcopy.html
   * http://stackoverflow.com/questions/5157764/java-detect-circular-references-during-custom-cloning
   *
   * @param obj the object to write
   * @param ref
   */
  private void writePolo(DataOutputStream buffer, Object obj, int ref) throws IOException {
    if (obj == null) {
      validateAndWriteType(buffer, obj);
      return;
    }
    Map<String, Object> data = new HashMap<>();
    Class<?> klass = obj.getClass();
    if (obj instanceof JsonNode) {
      if (obj instanceof ObjectNode) {
        Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) obj).fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> e = it.next();
          data.put(e.getKey(), e.getValue());
        }
      } else if (obj instanceof ArrayNode) {
        for (int i = 0; i < ((ArrayNode) obj).size(); i++) {
          data.put(String.valueOf(i), ((ArrayNode) obj).get(i));
        }
      } else {
        throw new IllegalStateException(format("Found %s, only array and object types are supported as POLOs",
          klass.getName()));
      }
    } else {
      writePoloFieldsViaReflection(klass, obj, data);
    }
    //if at least one field is allowed to be serialized
    buffer.writeByte(POLO); //type
    //write the POLO's reference number
    buffer.writeInt(ref);
    writeString(buffer, klass.getName()); //class name
    buffer.writeInt(data.size()); //size
    for (String key : data.keySet()) {
      Object value = data.get(key);
      writeString(buffer, key); //key payload must be a string
      validateAndWriteType(buffer, value); //value payload
    }
  }

  private void writePoloFieldsViaReflection(Class<?> klass, Object obj, Map<String, Object> data) {
    Class<BosonProperty> propertyClass = BosonProperty.class;
    boolean ignoreInheritedFields = false;
    if (klass.isAnnotationPresent(propertyClass)) {
      ignoreInheritedFields = klass.getAnnotation(propertyClass).ignoreInheritedFields();
    }
    //get ALL (private,private,protect,package) fields declared in the class - includes inherited fields
    Set<Field> fields = ReflectionUtil.getAllFields(new HashSet<>(), klass, 0);
    for (Field field : fields) {
      //if inherited fields are to be ignored then fields must be declared in the current class
      if (ignoreInheritedFields && klass != field.getDeclaringClass()) {
        continue;
      }
      if (Modifier.isTransient(field.getModifiers())) {
        continue; //user doesn't want field serialised
      }
      if (!serialiseFinalFields && Modifier.isFinal(field.getModifiers())) {
        continue; //no point in serializing final fields
      }
      field.setAccessible(true);
      boolean add = true;
      String name = field.getName();
      //add if annotated with BosonProperty
      if (field.isAnnotationPresent(propertyClass)) {
        BosonProperty ann = field.getAnnotation(propertyClass);
        if (ann != null && !ann.value().isEmpty()) {
          name = ann.value();
        }
        if ((ann != null) && ann.ignore()) {
          add = false;
        }
        //if configured to ignore inherited fields then
        //only fields declared in the object's class are allowed
        if ((ann != null) && ann.ignoreInheritedFields() && field.getDeclaringClass() != klass) {
          add = false;
        }
      }
      if (add) {
        try {
          data.put(name, field.get(obj));
        } catch (IllegalAccessException e) {
          log.warn(format("Unable to access field %s in class %s", field.getName(),
            field.getDeclaringClass().getName()), e);
        }
      }
    }
  }

  /**
   * @param buffer the buffer to write to
   * @param param  the param to write to the buffer
   */
  private void validateAndWriteType(DataOutputStream buffer, Object param) throws IOException {
    if (param == null) {
      writeNull(buffer);
    } else {
      if (param instanceof Byte) {
        writeByte(buffer, (Byte) param);
      } else if (param instanceof Short) {
        writeShort(buffer, (Short) param);
      } else if (param instanceof Integer) {
        writeInt(buffer, (Integer) param);
      } else if (param instanceof Long) {
        writeLong(buffer, (Long) param);
      } else if (param instanceof Float) {
        writeFloat(buffer, (Float) param);
      } else if (param instanceof Double) {
        writeDouble(buffer, (Double) param);
      } else if (param instanceof Boolean) {
        writeBoolean(buffer, (Boolean) param);
      } else if (param instanceof Character) {
        writeChar(buffer, (Character) param);
      } else if (param instanceof String) {
        writeString(buffer, (String) param);
      } else if (param instanceof TextNode) {
        writeString(buffer, ((TextNode) param).textValue());
      } else if (param instanceof ShortNode) {
        writeShort(buffer, ((ShortNode) param).shortValue());
      } else if (param instanceof IntNode) {
        writeInt(buffer, ((IntNode) param).intValue());
      } else if (param instanceof LongNode) {
        writeLong(buffer, ((LongNode) param).longValue());
      } else if (param instanceof DoubleNode) {
        writeDouble(buffer, ((DoubleNode) param).doubleValue());
      } else if (param instanceof FloatNode) {
        writeFloat(buffer, ((FloatNode) param).floatValue());
      } else if (param instanceof BooleanNode) {
        writeBoolean(buffer, ((BooleanNode) param).booleanValue());
      } else if (param instanceof NullNode) {
        writeNull(buffer);
      } else if (param instanceof BinaryNode) {
        writeByteArray(buffer, ((BinaryNode) param).binaryValue());
      } else if (param instanceof List) {
        writeList(buffer, ((List<Object>) param).iterator(), ((List<Object>) param).size());
      } else if (param instanceof Set) {
        writeSet(buffer, (Set<Object>) param);
      } else if (param instanceof Map) {
        writeMap(buffer, (Map<Object, Object>) param);
      } else if (param instanceof byte[]) {
        writeByteArray(buffer, (byte[]) param);
      } else if (param.getClass().isArray()) {
        writeArray(buffer, param);
      } else if (param instanceof Enum) {
        writeEnum(buffer, (Enum) param);
      } else {
        if (param instanceof Throwable) {
          throw new UnsupportedOperationException("Cannot serialize throwable", (Throwable) param);
        }
        //in reference list?
        //can't use param.hashCode because recursive objects will StackOverFlow computing it in some cases
        //e.g. Jackson's ObjectNode
        int systemHashCode = System.identityHashCode(param);
        Integer ref = references.get(systemHashCode);
        //no
        if (ref == null) {
          //assign unique reference number
          ref = reference.getAndIncrement();
          //add to reference list
          references.put(systemHashCode, ref);
          writePolo(buffer, param, ref);
        } else {
          //yes -  write reference
          writeReference(buffer, ref);
        }
      }
    }
  }

  private void writeReference(DataOutputStream buffer, Integer ref) throws IOException {
    //if the object has been written already then write a negative reference
    buffer.writeByte(REFERENCE);
    buffer.writeInt(ref);
  }
}

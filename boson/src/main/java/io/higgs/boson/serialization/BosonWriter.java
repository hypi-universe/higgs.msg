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
  private static final Logger log = LoggerFactory.getLogger(BosonWriter.class);
  private static final BosonWriter instance = new BosonWriter();

  public static class WriterCtx {
    static final byte version = 1;
    protected final HashMap<Integer, Integer> references = new HashMap<>();
    protected final AtomicInteger reference = new AtomicInteger();
    private boolean serialiseFinalFields;
    private ByteArrayOutputStream arr = new ByteArrayOutputStream();
    private DataOutputStream buffer = new DataOutputStream(arr);

    public WriterCtx() {
    }

    public WriterCtx(boolean serialiseFinalFields) {
      this.serialiseFinalFields = serialiseFinalFields;
    }
  }

  protected BosonWriter() {
  }

  public static BosonWriter getInstance() {
    return instance;
  }

  /**
   * Serialize any object to a series of bytes.
   *
   * @param msg the message to serialize
   * @return a series of bytes representing the message
   */
  public static byte[] encode(Object msg) {
    return encode(msg, new WriterCtx());
  }

  public static byte[] encode(Object msg, boolean serialiseFinalFields) {
    return encode(msg, new WriterCtx(serialiseFinalFields));
  }

  public static byte[] encode(Object msg, WriterCtx ctx) {
    return encode(msg, ctx, instance);
  }

  public static byte[] encode(Object msg, WriterCtx ctx, BosonWriter writer) {
    try {
      ctx.buffer.writeByte(WriterCtx.version);
      writer.validateAndWriteType(ctx, msg);
    } catch (IOException ioe) {
      throw new InvalidDataException("Serialisation error", ioe);
    }
    return ctx.arr.toByteArray(); //not sure there's a way to avoid the memory copy here
  }

  private void writeByte(WriterCtx ctx, byte b) throws IOException {
    ctx.buffer.writeByte(BYTE);
    ctx.buffer.writeByte(b);
  }

  private void writeNull(WriterCtx ctx) throws IOException {
    ctx.buffer.writeByte(NULL);
  }

  private void writeShort(WriterCtx ctx, short s) throws IOException {
    ctx.buffer.writeByte(SHORT);
    ctx.buffer.writeShort(s);
  }

  private void writeInt(WriterCtx ctx, int i) throws IOException {
    ctx.buffer.writeByte(INT);
    ctx.buffer.writeInt(i);
  }

  private void writeLong(WriterCtx ctx, long l) throws IOException {
    ctx.buffer.writeByte(LONG);
    ctx.buffer.writeLong(l);
  }

  private void writeFloat(WriterCtx ctx, float f) throws IOException {
    ctx.buffer.writeByte(FLOAT);
    ctx.buffer.writeFloat(f);
  }

  private void writeDouble(WriterCtx ctx, double d) throws IOException {
    ctx.buffer.writeByte(DOUBLE);
    ctx.buffer.writeDouble(d);
  }

  private void writeBoolean(WriterCtx ctx, boolean b) throws IOException {
    ctx.buffer.writeByte(BOOLEAN);
    if (b) {
      ctx.buffer.writeByte(1);
    } else {
      ctx.buffer.writeByte(0);
    }
  }

  private void writeChar(WriterCtx ctx, char c) throws IOException {
    ctx.buffer.writeByte(CHAR);
    ctx.buffer.writeChar(c);
  }

  private void writeString(WriterCtx ctx, String s) throws IOException {
    ctx.buffer.writeByte(STRING); //type
    byte[] str = s.getBytes(utf8);
    ctx.buffer.writeInt(str.length); //size
    ctx.buffer.write(str); //payload
  }

  private void writeEnum(WriterCtx ctx, Enum param) throws IOException {
    ctx.buffer.writeByte(ENUM); //type
    writeString(ctx, param.getClass().getName()); //enum class type
    writeString(ctx, param.toString()); //enum value
  }

  private void writeList(WriterCtx ctx, Iterator value, int size) throws IOException {
    ctx.buffer.writeByte(LIST); //type
    ctx.buffer.writeInt(size); //size
    while (value.hasNext()) {
      Object param = value.next();
      if (param == null) {
        writeNull(ctx);
      } else {
        validateAndWriteType(ctx, param); //payload
      }
    }
  }

  private void writeSet(WriterCtx ctx, Set<Object> value) throws IOException {
    ctx.buffer.writeByte(SET); //type
    ctx.buffer.writeInt(value.size()); //size
    for (Object param : value) {
      if (param == null) {
        writeNull(ctx);
      } else {
        validateAndWriteType(ctx, param); //payload
      }
    }
  }

  /**
   * Write an array of any supported boson type to the given buffer.
   * If the buffer contains any unsupported type, this will fail by throwing an UnsupportedBosonTypeException
   *
   * @param value the value to write
   */
  private void writeArray(WriterCtx ctx, Object value) throws IOException {
    ctx.buffer.writeByte(ARRAY); //type
    int length = Array.getLength(value);
    ctx.buffer.writeInt(length); //size
    writeString(ctx, value.getClass().getComponentType().getName()); //component type
    for (int i = 0; i < length; i++) {
      validateAndWriteType(ctx, Array.get(value, i)); //payload
    }
  }

  private void writeByteArray(WriterCtx ctx, byte[] value) throws IOException {
    ctx.buffer.writeByte(BYTE_ARRAY); //type
    ctx.buffer.writeInt(value.length); //size
    ctx.buffer.write(value); //payload
  }

  private void writeMap(WriterCtx ctx, Map<?, ?> value) throws IOException {
    ctx.buffer.writeByte(MAP); //type
    ctx.buffer.writeInt(value.size()); //size
    for (Object key : value.keySet()) {
      Object v = value.get(key);
      validateAndWriteType(ctx, key); //key payload
      validateAndWriteType(ctx, v); //value payload
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
  private void writePolo(WriterCtx ctx, Object obj, int ref) throws IOException {
    if (obj == null) {
      validateAndWriteType(ctx, obj);
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
        throw new IllegalStateException(format(
          "Found %s, only array and object types are supported as POLOs",
          klass.getName()
        ));
      }
    } else {
      writePoloFieldsViaReflection(ctx, klass, obj, data);
    }
    //if at least one field is allowed to be serialized
    ctx.buffer.writeByte(POLO); //type
    //write the POLO's reference number
    ctx.buffer.writeInt(ref);
    writeString(ctx, klass.getName()); //class name
    ctx.buffer.writeInt(data.size()); //size
    for (String key : data.keySet()) {
      Object value = data.get(key);
      writeString(ctx, key); //key payload must be a string
      validateAndWriteType(ctx, value); //value payload
    }
  }

  private void writePoloFieldsViaReflection(WriterCtx ctx, Class<?> klass, Object obj, Map<String, Object> data) {
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
      if (!ctx.serialiseFinalFields && Modifier.isFinal(field.getModifiers())) {
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
            field.getDeclaringClass().getName()
          ), e);
        }
      }
    }
  }

  /**
   * @param ctx   the writer ctx
   * @param param the param to write to the buffer
   */
  private void validateAndWriteType(WriterCtx ctx, Object param) throws IOException {
    if (param == null) {
      writeNull(ctx);
    } else {
      if (param instanceof Byte) {
        writeByte(ctx, (Byte) param);
      } else if (param instanceof Short) {
        writeShort(ctx, (Short) param);
      } else if (param instanceof Integer) {
        writeInt(ctx, (Integer) param);
      } else if (param instanceof Long) {
        writeLong(ctx, (Long) param);
      } else if (param instanceof Float) {
        writeFloat(ctx, (Float) param);
      } else if (param instanceof Double) {
        writeDouble(ctx, (Double) param);
      } else if (param instanceof Boolean) {
        writeBoolean(ctx, (Boolean) param);
      } else if (param instanceof Character) {
        writeChar(ctx, (Character) param);
      } else if (param instanceof String) {
        writeString(ctx, (String) param);
      } else if (param instanceof TextNode) {
        writeString(ctx, ((TextNode) param).textValue());
      } else if (param instanceof ShortNode) {
        writeShort(ctx, ((ShortNode) param).shortValue());
      } else if (param instanceof IntNode) {
        writeInt(ctx, ((IntNode) param).intValue());
      } else if (param instanceof LongNode) {
        writeLong(ctx, ((LongNode) param).longValue());
      } else if (param instanceof DoubleNode) {
        writeDouble(ctx, ((DoubleNode) param).doubleValue());
      } else if (param instanceof FloatNode) {
        writeFloat(ctx, ((FloatNode) param).floatValue());
      } else if (param instanceof BooleanNode) {
        writeBoolean(ctx, ((BooleanNode) param).booleanValue());
      } else if (param instanceof NullNode) {
        writeNull(ctx);
      } else if (param instanceof BinaryNode) {
        writeByteArray(ctx, ((BinaryNode) param).binaryValue());
      } else if (param instanceof List) {
        writeList(ctx, ((List<Object>) param).iterator(), ((List<Object>) param).size());
      } else if (param instanceof Set) {
        writeSet(ctx, (Set<Object>) param);
      } else if (param instanceof Map) {
        writeMap(ctx, (Map<Object, Object>) param);
      } else if (param instanceof byte[]) {
        writeByteArray(ctx, (byte[]) param);
      } else if (param.getClass().isArray()) {
        writeArray(ctx, param);
      } else if (param instanceof Enum) {
        writeEnum(ctx, (Enum) param);
      } else {
        if (param instanceof Throwable) {
          throw new UnsupportedOperationException("Cannot serialize throwable", (Throwable) param);
        }
        //in reference list?
        //can't use param.hashCode because recursive objects will StackOverFlow computing it in some cases
        //e.g. Jackson's ObjectNode
        int systemHashCode = System.identityHashCode(param);
        Integer ref = ctx.references.get(systemHashCode);
        //no
        if (ref == null) {
          //assign unique reference number
          ref = ctx.reference.getAndIncrement();
          //add to reference list
          ctx.references.put(systemHashCode, ref);
          writePolo(ctx, param, ref);
        } else {
          //yes -  write reference
          writeReference(ctx, ref);
        }
      }
    }
  }

  private void writeReference(WriterCtx ctx, Integer ref) throws IOException {
    //if the object has been written already then write a negative reference
    ctx.buffer.writeByte(REFERENCE);
    ctx.buffer.writeInt(ref);
  }
}

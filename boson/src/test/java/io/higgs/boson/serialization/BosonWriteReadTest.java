package io.higgs.boson.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BosonWriteReadTest {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void readWriteWithJSON() {
    BosonWriter writer = new BosonWriter();
    BosonReader reader = new BosonReader();
    Map<String, Object> data = new HashMap<>();
    data.put("short", (short) 1);
    data.put("int", 1);
    data.put("long", 1L);
    data.put("double", 1.2D);
    data.put("float", 1.3F);
    data.put("str", "text");
    data.put("bool", true);
    data.put("null", null);
    byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6, 7};
    data.put("byte_arr", bytes);
    ObjectNode obj = mapper.convertValue(data, ObjectNode.class);
    obj.set("ref", obj);
    ArrayNode arrPrim = mapper.createArrayNode();
    ArrayNode arrObjs = mapper.createArrayNode();
    ArrayNode arrMixed = mapper.createArrayNode();

    arrPrim.add(1).add(2).add(3);
    arrObjs.add(obj).add(obj);
    arrMixed.add(1).add(obj);

    byte[] objWritten = writer.serialize(obj);
    byte[] arrPrimWritten = writer.serialize(arrPrim);
    byte[] arrObjsWritten = writer.serialize(arrObjs);
    byte[] arrMixedWritten = writer.serialize(arrMixed);

    ObjectNode objRead = reader.deSerialise(objWritten);
    ArrayNode arrPrimRead = reader.deSerialise(arrPrimWritten);
    ArrayNode arrObjRead = reader.deSerialise(arrObjsWritten);
    ArrayNode arrMixedRead = reader.deSerialise(arrMixedWritten);

    assertEquals(arrPrimRead.toString(), arrPrim.toString());
    assertEquals(obj.get("byte_arr"), objRead.get("byte_arr"));
    assertEquals(obj.get("short"), objRead.get("short"));
    assertEquals(obj.get("int"), objRead.get("int"));
    assertEquals(obj.get("long"), objRead.get("long"));
    assertEquals(obj.get("double"), objRead.get("double"));
    assertEquals(obj.get("float"), objRead.get("float"));
    assertEquals(obj.get("str"), objRead.get("str"));
    assertEquals(obj.get("bool"), objRead.get("bool"));
    assertEquals(obj.get("null"), objRead.get("null"));
    //when a ref is read, it's stored and then the same instance is used whereever it is refered to so the system
    //identity hash should match so since ref is recursive, when we get ref, it should return an object which
    //points to itself so getting ref again gives the same object
    assertEquals(System.identityHashCode(objRead.get("ref").get("ref")),
      System.identityHashCode(objRead.get("ref")));

    //Jackson can't serialise the tree because ref is recursive so let's remove it and assert the rest is equal
    //we already test for ref being correct above
    obj.remove("ref");
    objRead.remove("ref");
    assertEquals(obj.toString(), objRead.toString());
    ((ObjectNode) arrObjs.get(0)).remove("ref");
    ((ObjectNode) arrObjs.get(1)).remove("ref");
    ((ObjectNode) arrObjRead.get(0)).remove("ref");
    ((ObjectNode) arrObjRead.get(1)).remove("ref");
    assertEquals(arrObjRead.toString(), arrObjs.toString());

    arrMixed.remove(1);
    arrMixedRead.remove(1);
    assertEquals(arrMixedRead.toString(), arrMixed.toString());
  }
}

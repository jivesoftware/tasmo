package com.jivesoftware.os.tasmo.reference.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.tasmo.id.JsonNodeMarshaller;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.ObjectIdMarshaller;
import org.testng.annotations.DataProvider;

/**
 *
 *
 */
public class MarshallerTestDataProvider {

    @DataProvider(name = "createBytes")
    public static Object[][] createBytes() throws Exception {

        String className1 = "amazon";
        long id1 = 300000;

        String className2 = "雅虎邮箱";
        String fieldName2 = "mail";

        String className3 = "classname";
        String fieldName3 = "firstName";

        ObjectId objectId = new ObjectId(className1, new Id(id1));
        ObjectIdMarshaller marshaller1 = new ObjectIdMarshaller();
        byte[] bytes1 = marshaller1.toBytes(objectId);

        ClassAndField_IdKey key1 = new ClassAndField_IdKey(className2, fieldName2, objectId);
        ClassAndField_IdKeyMarshaller marshaller2 = new ClassAndField_IdKeyMarshaller();
        byte[] bytes2 = marshaller2.toBytes(key1);

        ClassAndFieldKey key2 = new ClassAndFieldKey(className3, fieldName3);
        ClassAndFieldKeyMarshaller marshaller3 = new ClassAndFieldKeyMarshaller();
        byte[] bytes3 = marshaller3.toBytes(key2);

        String str = "this";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.convertValue(str, JsonNode.class);
        JsonNodeMarshaller marshaller4 = new JsonNodeMarshaller();
        byte[] bytes4 = marshaller4.toBytes(jsonNode);

        ImmutableByteArray array1 = new ImmutableByteArray("my array");
        ImmutableByteArrayMarshaller marshaller5 = new ImmutableByteArrayMarshaller();
        byte[] bytes5 = marshaller5.toBytes(array1);

        return new Object[][] {
            //objectId type bytes
            { bytes1, "ObjectId" },
            //ClassAndField_IdKey type bytes
            { bytes2, "ClassAndField_IdKey" },
            //ClassAndFieldKey type bytes
            { bytes3, "ClassAndFieldKey" },
            //String type bytes
            { bytes4, "String" },
            //immutableArray type bytes
            { bytes5, "ImmutableByteArray" }
        };
    }
}

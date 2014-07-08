package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import org.testng.annotations.DataProvider;

public class MarshallerTestDataProvider {

    @DataProvider(name = "createBytes")
    public static Object[][] createBytes() throws Exception {

        String className1 = "amazon";
        long id1 = 300_000;

        ObjectId objectId = new ObjectId(className1, new Id(id1));
        ObjectIdMarshaller marshaller1 = new ObjectIdMarshaller();
        byte[] bytes1 = marshaller1.toBytes(objectId);

        String str = "this";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.convertValue(str, JsonNode.class);
        JsonNodeMarshaller marshaller4 = new JsonNodeMarshaller();
        byte[] bytes2 = marshaller4.toBytes(jsonNode);

        ImmutableByteArray array1 = new ImmutableByteArray("my array");
        ImmutableByteArrayMarshaller marshaller5 = new ImmutableByteArrayMarshaller();
        byte[] bytes3 = marshaller5.toBytes(array1);

        return new Object[][] {
            //objectId type bytes
            { bytes1 },
            //String type bytes
            { bytes2 },
            //immutableArray type bytes
            { bytes3 }
        };
    }
}

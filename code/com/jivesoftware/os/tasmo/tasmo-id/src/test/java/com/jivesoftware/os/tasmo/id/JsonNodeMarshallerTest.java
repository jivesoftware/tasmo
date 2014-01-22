package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JsonNodeMarshallerTest {

    @Test(dataProviderClass = ObjectIdTestDataProvider.class, dataProvider = "createObjectId")
    public void testMarshaller(String className, long longId) throws Exception {
        JsonNodeMarshaller jsonNodeMarshaller = new JsonNodeMarshaller();

        ObjectId objectId1 = new ObjectId(className, new Id(longId));
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(objectId1);
        JsonNode jsonNode = mapper.readTree(json);

        //    JsonNode node = objectMapper.valueToTree(map);
        //    JsonNode node = mapper.convertValue(object, JsonNode.class);

        byte[] bytes = jsonNodeMarshaller.toBytes(jsonNode);
        JsonNode jsonNode1 = jsonNodeMarshaller.fromBytes(bytes);

        ObjectId objectId2 = mapper.convertValue(jsonNode1, ObjectId.class);

        System.out.println("before JsonNodeMarshaller: objectId=" + objectId1.toStringForm());
        System.out.println("after JsonNodeMarshaller : objectId=" + objectId2.toStringForm());

        Assert.assertTrue(objectId1.equals(objectId2), " marshalled object should be equal to the original");
    }

    @Test
    public void testMarshaller_null() throws Exception {
        JsonNodeMarshaller jsonNodeMarshaller = new JsonNodeMarshaller();

        JsonNode jsonNode = jsonNodeMarshaller.fromBytes(null);

        Assert.assertNull(jsonNode, " marshalled object from null bytes is null");
    }
}

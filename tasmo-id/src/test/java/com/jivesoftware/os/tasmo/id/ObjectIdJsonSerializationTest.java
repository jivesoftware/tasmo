package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 *
 */
public class ObjectIdJsonSerializationTest {
    @Test
    public void testSerializeDeserialize() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectId objectId = new ObjectId("abc", new Id(1L));
        String serializedObjectId = objectMapper.writeValueAsString(objectId);
        System.out.println(serializedObjectId);
        ObjectId deserializedObjectId = objectMapper.readValue(serializedObjectId, ObjectId.class);
        System.out.println(objectId.toStringForm() + " vs " + deserializedObjectId.toStringForm());
        System.out.println(objectId.equals(deserializedObjectId));
        assertEquals(objectId, deserializedObjectId);
        System.out.flush();
    }
}

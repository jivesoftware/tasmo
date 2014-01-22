package com.jivesoftware.os.tasmo.id;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjectIdMarshallerTest {

    @Test(dataProviderClass = ObjectIdTestDataProvider.class, dataProvider = "createObjectId")
    public void testMarshaller(String className, long longId) throws Exception {
        ObjectId id1 = new ObjectId(className, new Id(longId));

        ObjectIdMarshaller marshaller = new ObjectIdMarshaller();
        byte[] bytes = marshaller.toBytes(id1);

        ObjectId id2 = marshaller.fromBytes(bytes);

        System.out.println("before ObjectIdMarshaller: ObjectId =" + id1.toStringForm());
        System.out.println("after ObjectIdMarshaller: ObjectId=" + id2.toStringForm());
        Assert.assertTrue(id1.equals(id2), " marshalled object should be equal to the original");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMarshaller_null() throws Exception {
        ObjectIdMarshaller marshaller = new ObjectIdMarshaller();
        marshaller.fromBytes(null);
    }
}

package com.jivesoftware.os.tasmo.id;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TenantIdAndCentricIdMarshallerTest {

    @Test
    public void testMarshaller() throws Exception {
        TenantIdAndCentricId id1 = new TenantIdAndCentricId(new TenantId("booya"), new Id(1000));

        TenantIdAndCentricIdMarshaller marshaller = new TenantIdAndCentricIdMarshaller();
        byte[] bytes = marshaller.toBytes(id1);

        TenantIdAndCentricId id2 = marshaller.fromBytes(bytes);

        System.out.println("before ObjectIdMarshaller: ObjectId =" + id1.toString());
        System.out.println("after ObjectIdMarshaller: ObjectId=" + id2.toString());
        Assert.assertTrue(id1.equals(id2), " marshalled object should be equal to the original");
    }

}

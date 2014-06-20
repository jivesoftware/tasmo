package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ImmutableByteArrayMarshallerTest {

    @Test(dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString")
    public void testMarshaller(String string) throws Exception {
        ImmutableByteArray key1 = new ImmutableByteArray(string);

        System.out.println("before ImmutableByteArrayMarshaller: " + string);

        ImmutableByteArrayMarshaller marshaller = new ImmutableByteArrayMarshaller();
        byte[] bytes = marshaller.toBytes(key1);

        ImmutableByteArray key2 = marshaller.fromBytes(bytes);

        System.out.println("after ImmutableByteArrayMarshaller: toString()=" + key2.toString());

        Assert.assertTrue(key1.equals(key2), "marshalled object should be equal to the original");
    }

    @Test(dataProviderClass = MarshallerTestDataProvider.class, dataProvider = "createBytes")
    public void testMarshallerOtherInput(byte[] differentTypeBytes) throws Exception {

        ImmutableByteArrayMarshaller marshaller = new ImmutableByteArrayMarshaller();
        ImmutableByteArray obj2 = marshaller.fromBytes(differentTypeBytes);

        System.out.println("after ImmutableByteArrayMarshaller:  =" + obj2.toString());

        Assert.assertEquals(obj2.getImmutableBytes(), differentTypeBytes);

    }
}

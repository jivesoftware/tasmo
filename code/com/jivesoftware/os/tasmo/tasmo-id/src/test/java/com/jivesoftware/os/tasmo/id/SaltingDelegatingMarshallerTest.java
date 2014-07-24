package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class SaltingDelegatingMarshallerTest {

    @Test(dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString")
    public void testPrefixSaltingMarshaller(String string) throws Exception {
        doTestSaltingMarshaller(string, true);
    }

    @Test(dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString")
    public void testSuffixSaltingMarshaller(String string) throws Exception {
        doTestSaltingMarshaller(string, false);
    }

    private void doTestSaltingMarshaller(String string, boolean saltPrefixNotSuffix) throws Exception {
        ImmutableByteArray key = new ImmutableByteArray(string);

        ImmutableByteArrayMarshaller marshaller = new ImmutableByteArrayMarshaller();
        byte[] bytes = marshaller.toBytes(key);

        SaltingDelegatingMarshaller<ImmutableByteArrayMarshaller, ImmutableByteArray> saltingMarshaller =
                new SaltingDelegatingMarshaller<>(marshaller, saltPrefixNotSuffix);

        byte[] saltedBytes = saltingMarshaller.toBytes(key);

        Assert.assertEquals(saltedBytes.length - bytes.length, SaltingDelegatingMarshaller.SALT_SIZE_IN_BYTES,
                "Salted bytes length should be greater by " + SaltingDelegatingMarshaller.SALT_SIZE_IN_BYTES);

        ImmutableByteArray keyFromSaltedBytes = saltingMarshaller.fromBytes(saltedBytes);

        Assert.assertTrue(key.equals(keyFromSaltedBytes), "Key from salted bytes should be equal to the original");
    }
}

package com.jivesoftware.os.tasmo.id;

import java.nio.charset.Charset;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ImmutableByteArrayTest {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString")
    public void testConstructor_string(String string) throws Exception {
        ImmutableByteArray array1 = new ImmutableByteArray(string);

        Assert.assertEquals(array1.toString(), string, "toString() should get the same string as original");
        Assert.assertEquals(string.getBytes("UTF-8").length, array1.length(), "length()");
    }

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createBytes")
    public void testConstructor_bytes(byte[] bytes, String string, int len) {
        ImmutableByteArray array1 = new ImmutableByteArray(bytes);

        Assert.assertEquals(array1.toString(), string, "toString()");
        Assert.assertEquals(array1.length(), len, "length()");
    }

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createMultiples")
    public void testConstructor_strings(byte[] bytes1, String separator, byte[] bytes2, String string, int len) {
        ImmutableByteArray a = new ImmutableByteArray(bytes1);
        ImmutableByteArray b = new ImmutableByteArray(bytes2);
        byte[] bs = join(a.getImmutableBytes(), separator.getBytes(UTF8), b.getImmutableBytes());

        ImmutableByteArray array1 = new ImmutableByteArray(bs);

        Assert.assertEquals(array1.toString(), string, "toString()");
        Assert.assertEquals(array1.length(), len, "length()");
        Assert.assertTrue(array1.startsWith(bytes1), "this array should start with this bytes");

    }

    byte[] join(byte[] a, byte[] b, byte[] c) {
        byte[] newSrc = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, newSrc, 0, a.length);
        System.arraycopy(b, 0, newSrc, a.length, b.length);
        System.arraycopy(c, 0, newSrc, a.length + b.length, c.length);
        return newSrc;
    }

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createPair")
    public void testEquals(String string1, String string2, boolean expectedEqual,
        int expectedCompare) {
        ImmutableByteArray array1 = new ImmutableByteArray(string1);
        ImmutableByteArray array2 = new ImmutableByteArray(string2);

        Assert.assertEquals(array1.equals(array2), expectedEqual, "are equal meet the expectation?");
        if (expectedEqual) {
            Assert.assertEquals(array1.compareTo(array2), 0, "equal means compare == 0");
            Assert.assertEquals(array1.hashCode(), array2.hashCode(), "equals means same hashcode");
        }

        Assert.assertEquals(array1.compareTo(array2), expectedCompare, " are compare meet the expectation");

        //noinspection ObjectEqualsNull
        Assert.assertFalse(array1.equals(null), "null object is not equal to any object");
        Assert.assertTrue(array1.equals(array1), "object is equal to self");
        Assert.assertFalse(array1.equals(new ObjectId("myclass", new Id(2))), "other type of object is not equal to this kind of object");
        Assert.assertTrue(array1.equals(array1.getImmutableBytes()), "only bytes type of object can be compared by this object");
        Assert.assertEquals(array1.compareTo(array1.getImmutableBytes()), 0, "only bytes type of object can be compared by this object");
    }

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString")
    public void testEquals_null(String string1) {
        ImmutableByteArray array1 = new ImmutableByteArray(string1);

        //noinspection ObjectEqualsNull
        Assert.assertFalse(array1.equals(null), "are equal meet the expectation?");
    }

    @Test (dataProviderClass = ImmutableByteArrayTestDataProvider.class, dataProvider = "createString",
        expectedExceptions = java.lang.NullPointerException.class)
    public void testCompare_null(String string1) {
        ImmutableByteArray array1 = new ImmutableByteArray(string1);
        array1.compareTo(null);
    }
}

package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassAndFieldKeyTest {

    @Test(dataProviderClass = ClassAndFieldTestDataProvider.class, dataProvider = "createClassFieldPair")
    public void testEquals(String className1, String fieldName1, String className2, String fieldName2, boolean expectedEqual,
        int expectedCompare) {
        ClassAndFieldKey key1 = new ClassAndFieldKey(className1, fieldName1);
        ClassAndFieldKey key2 = new ClassAndFieldKey(className2, fieldName2);

        Assert.assertEquals(expectedEqual, key1.equals(key2), "are equal meet the expectation?");
        if (expectedEqual) {
            Assert.assertEquals(0, key1.compareTo(key2), "equal means compare == 0");
            Assert.assertEquals(key1.hashCode(), key2.hashCode(), "equals means same hashcode");
        }

        if (expectedCompare < 0) {
            Assert.assertTrue(key1.compareTo(key2) < 0, "are compare <0?" + key1.compareTo(key2));
        } else if (expectedCompare > 0) {
            Assert.assertTrue(key1.compareTo(key2) > 0, "are compare > 0?");
        } else {
            Assert.assertTrue(key1.compareTo(key2) == 0, "are compare ==0 ?");
        }

        Assert.assertEquals(key1 == null, false, "null object is not equal to any object");
        Assert.assertEquals(key1.equals(key1), true, "object is equal to self object");
        Assert.assertEquals(key1.equals(new ObjectId("myclass", new Id(2))), false, "other type of object is not equal to this kind of object");
    }
}

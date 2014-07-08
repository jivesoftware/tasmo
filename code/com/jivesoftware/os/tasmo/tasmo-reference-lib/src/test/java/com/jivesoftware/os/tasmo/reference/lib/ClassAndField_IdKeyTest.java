package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassAndField_IdKeyTest {

    @Test(dataProviderClass = ClassAndFieldTestDataProvider.class, dataProvider = "createClassFieldIdPair")
    public void testEquals(String className1, String fieldName1, ObjectId id1, String className2, String fieldName2, ObjectId id2, boolean expectedEqual,
        int expectedCompare) {
        ClassAndField_IdKey key1 = new ClassAndField_IdKey(className1, fieldName1, id1);
        ClassAndField_IdKey key2 = new ClassAndField_IdKey(className2, fieldName2, id2);

        Assert.assertEquals(expectedEqual, key1.equals(key2), "are equal meet the expectation?");
        if (expectedEqual) {
            Assert.assertEquals(0, key1.compareTo(key2), "equal means compare == 0");
            Assert.assertEquals(key1.hashCode(), key2.hashCode(), "equals means same hashcode");
        }

        if (expectedCompare < 0) {
            Assert.assertTrue(key1.compareTo(key2) < 0, "are compare <0?");
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

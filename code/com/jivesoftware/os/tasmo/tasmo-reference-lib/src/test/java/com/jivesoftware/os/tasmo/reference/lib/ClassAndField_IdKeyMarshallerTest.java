package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.tasmo.id.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassAndField_IdKeyMarshallerTest {

    @Test(dataProviderClass = ClassAndFieldTestDataProvider.class, dataProvider = "createClassFieldId")
    public void testMarshaller(String className, String fieldName, ObjectId id) throws Exception {
        ClassAndField_IdKey key1 = new ClassAndField_IdKey(className, fieldName, id);

        ClassAndField_IdKeyMarshaller marshaller = new ClassAndField_IdKeyMarshaller();
        byte[] bytes = marshaller.toBytes(key1);

        ClassAndField_IdKey key2 = marshaller.fromBytes(bytes);

        System.out.println("before ClassAndField_IdKeyMarshaller: classname=" + key1.getClassName());
        System.out.println("before ClassAndField_IdKeyMarshaller: fieldname=" + key1.getFieldName());
        System.out.println("before ClassAndField_IdKeyMarshaller: objectId =" + key1.getObjectId());

        System.out.println("after ClassAndField_IdKeyMarshaller: classname=" + key2.getClassName());
        System.out.println("after ClassAndField_IdKeyMarshaller: fieldname=" + key2.getFieldName());
        System.out.println("after ClassAndField_IdKeyMarshaller: objectId=" + key2.getObjectId());

        Assert.assertTrue(key1.equals(key2), " marshalled object should be equal to the original");
    }

    @Test(dataProviderClass = MarshallerTestDataProvider.class, dataProvider = "createBytes")
    public void testMarshallerBadInput(byte[] differentTypeBytes, String className) throws Exception {
        ClassAndField_IdKeyMarshaller marshaller = new ClassAndField_IdKeyMarshaller();
        boolean caughtException = false;
        try {
            ClassAndField_IdKey obj2 = marshaller.fromBytes(differentTypeBytes);
            System.out.println("after ClassAndField_IdKeyMarshaller:  =" + obj2.toString());
        } catch (Exception e) {
            caughtException = true;
        }
        Assert.assertEquals(caughtException, !className.equals(ClassAndField_IdKey.class.getSimpleName()));
    }
}

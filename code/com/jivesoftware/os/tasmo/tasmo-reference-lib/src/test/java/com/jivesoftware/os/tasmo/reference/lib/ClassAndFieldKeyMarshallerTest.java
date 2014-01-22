/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class ClassAndFieldKeyMarshallerTest {

    @Test(dataProviderClass = ClassAndFieldTestDataProvider.class, dataProvider = "createClassField")
    public void testMarshaller(String className, String fieldName) throws Exception {
        ClassAndFieldKey key1 = new ClassAndFieldKey(className, fieldName);

        System.out.println("before ClassAndFieldKeyMarshaller: classname=" + key1.getClassName());
        System.out.println("before ClassAndFieldKeyMarshaller: fieldname=" + key1.getFieldName());

        ClassAndFieldKeyMarshaller marshaller = new ClassAndFieldKeyMarshaller();
        byte[] bytes = marshaller.toBytes(key1);

        ClassAndFieldKey key2 = marshaller.fromBytes(bytes);

        System.out.println("after ClassAndFieldKeyMarshaller: classname=" + key2.getClassName());
        System.out.println("after ClassAndFieldKeyMarshaller: fieldname=" + key2.getFieldName());

        Assert.assertTrue(key1.equals(key2), " marshalled object should be equal to the original");
    }

    @Test(dataProviderClass = MarshallerTestDataProvider.class, dataProvider = "createBytes")
    public void testMarshallerBadInput(byte[] differentTypeBytes, String className) throws Exception {
        ClassAndFieldKeyMarshaller marshaller = new ClassAndFieldKeyMarshaller();
        boolean caughtException = false;
        try {
            ClassAndFieldKey obj2 = marshaller.fromBytes(differentTypeBytes);

            System.out.println("after ClassAndFieldKeyMarshaller:  =" + obj2.toString());
        } catch (Exception e) {
            caughtException = true;
        }
        Assert.assertEquals(caughtException, !className.startsWith("ClassAndField"));
    }

}

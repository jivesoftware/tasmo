package com.jivesoftware.os.tasmo.id;

import org.testng.annotations.DataProvider;

public class ObjectIdTestDataProvider {

    @DataProvider(name = "createObjectId")
    public static Object[][] createObjectId() {

        String className1 = "amazon";
        long id1 = 300000;

        String className2 = "雅虎邮箱";
        long id2 = 20000;

        return new Object[][] {
            { className1, id1 },
            { className2, id2 },
        };
    }
}

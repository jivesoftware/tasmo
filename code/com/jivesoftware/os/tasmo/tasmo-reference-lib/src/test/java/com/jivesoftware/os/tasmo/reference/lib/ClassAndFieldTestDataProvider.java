package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import org.testng.annotations.DataProvider;

public class ClassAndFieldTestDataProvider {

    @DataProvider(name = "createClassFieldIdPair")
    public static Object[][] createClassFieldIdPair() {

        String className1 = "amazon";
        String fieldName1 = "m3";
        ObjectId id1 = new ObjectId("buyer", new Id(1));

        String className2 = "amazon";
        String fieldName2 = "m3";
        ObjectId id2 = new ObjectId("buyer", new Id(1));

        String className5 = "amazon";
        String fieldName5 = "m2";
        ObjectId id5 = new ObjectId("buyer", new Id(1));

        String className6 = "amazon";
        String fieldName6 = "m3";
        ObjectId id6 = new ObjectId("buyer", new Id(3));

        String className3 = "雅虎邮箱";
        String fieldName3 = "mail";
        ObjectId id3 = new ObjectId("Email", new Id(0));

        String className4 = "雅虎邮箱";
        String fieldName4 = "mail";
        ObjectId id4 = new ObjectId("Email", new Id(0));

        return new Object[][] {
            //same
            { className1, fieldName1, id1, className2, fieldName2, id2, true, 0 },
            //self
            { className1, fieldName1, id1, className1, fieldName1, id1, true, 0 },
            //same
            { className3, fieldName3, id3, className4, fieldName4, id4, true, 0 },
            //self
            { className3, fieldName3, id3, className3, fieldName3, id3, true, 0 },
            //diff
            { className1, fieldName1, id1, className3, fieldName3, id3, false, -1 },

            { className1, fieldName1, id1, className5, fieldName5, id5, false, 1 },

            { className1, fieldName1, id1, className6, fieldName6, id6, false, -1 },
        };
    }

    @DataProvider(name = "createClassFieldId")
    public static Object[][] createClassFieldId() {

        String className1 = "amazon";
        String fieldName1 = "m3";
        ObjectId id1 = new ObjectId("buyer", new Id(1));

        String className2 = "amazon";
        String fieldName2 = "m3";
        ObjectId id2 = new ObjectId("邮箱_", new Id(1));

        String className3 = "雅虎邮箱";
        String fieldName3 = "मेल";
        ObjectId id3 = new ObjectId("Email_", new Id(0));

        return new Object[][] {
            { className1, fieldName1, id1 },
            { className2, fieldName2, id2 },
            { className3, fieldName3, id3 }
        };
    }

    @DataProvider(name = "createClassFieldPair")
    public static Object[][] createClassFieldPair() {

        String className1 = "amazon";
        String fieldName1 = "m3";

        String className2 = "amazon";
        String fieldName2 = "m3";

        String className5 = "amazon";
        String fieldName5 = "m2";

        String className3 = "雅虎邮箱";
        String fieldName3 = "mail";

        String className4 = "雅虎邮箱";
        String fieldName4 = "mail";

        return new Object[][] {
            { className1, fieldName1, className2, fieldName2, true, 0 },
            { className1, fieldName1, className1, fieldName1, true, 0 },
            { className3, fieldName3, className4, fieldName4, true, 0 },
            { className3, fieldName3, className3, fieldName3, true, 0 },
            { className1, fieldName1, className3, fieldName3, false, -1 },
            { className1, fieldName1, className5, fieldName5, false, 1 },

        };
    }

    @DataProvider(name = "createClassField")
    public static Object[][] createClassField() {

        String className1 = "amazon";
        String fieldName1 = "m3";

        String className2 = "雅虎邮箱";
        String fieldName2 = "m3";

        String className3 = "amazon";
        String fieldName3 = "मेल";

        return new Object[][] {
            { className1, fieldName1 },
            { className2, fieldName2 },
            { className3, fieldName3 }
        };
    }

}

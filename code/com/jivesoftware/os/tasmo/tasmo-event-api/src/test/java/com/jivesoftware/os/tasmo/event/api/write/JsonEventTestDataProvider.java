package com.jivesoftware.os.tasmo.event.api.write;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.ArrayList;
import java.util.LinkedList;
import org.testng.annotations.DataProvider;

public class JsonEventTestDataProvider {

    @DataProvider(name = "createJsonData")
    public static Object[][] createJsonData() {
        String key1 = "StringKey";
        String value1 = "My Key1";

        String key2 = "I18Key";
        String value2 = "坚持勤俭办一切事业";

        String key3 = "ObjectIdKey";
        ObjectId value3 = new ObjectId("Myclass", new Id(100));

        String key4 = "ObjectIdsKey";
        LinkedList<ObjectId> value4 = new LinkedList<>();

        String key5 = "ObjectIdsKey";
        LinkedList<ObjectId> value5 = new LinkedList<>();
        value5.add(new ObjectId("myClass1", new Id(1)));
        value5.add(new ObjectId("myClass2", new Id(2)));

        String key6 = "ObjectIdsKey";
        ArrayList<ObjectId> value6 = new ArrayList<>();
        value6.add(new ObjectId("myClass1", new Id(1)));
        value6.add(new ObjectId("myClass2", new Id(2)));

        Object[][] dataObj = new Object[][] {
            { key1, value1 },
            { key2, value2 },
            { key3, value3 },
            { key4, value4 },
            { key5, value5 },
            { key6, value6 }
        };

        return dataObj;
    }

    @DataProvider(name = "createBadJsonData")
    public static Object[][] createBadJsonData() {
        //null value
        String key1 = "ObjectIdKey";
        ObjectId value1 = null;

        //mixed objectid and other type
        String key2 = "ObjectIdsKey";
        LinkedList<Object> value2 = new LinkedList<>();

        value2.add(new ObjectId("something", new Id(2343)));
        value2.add("my string");

        Object[][] dataObj = new Object[][] {
            { key1, value1 },
            { key2, value2 }
        };

        return dataObj;
    }
}

package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.io.IOException;
import org.testng.annotations.DataProvider;

public class ImmutableByteArrayTestDataProvider {

    @DataProvider (name = "createString")
    public static Object[][] createString() {

        String string1 = "amazon";
        String string2 = "雅虎邮箱";
        String string3 = "";
        String string4 = ImmutableByteArray.NULL.toString();
        String string5 = null;

        return new Object[][]{
            { string1 },
            { string2 },
            { string3 },
            { string4 },
            //this will get trouble
            { string4 }
        };
    }

    @DataProvider (name = "createBytes")
    public static Object[][] createBytes() throws IOException {

        String string1 = "amazon";
        byte[] bytes1 = string1.getBytes("UTF-8");

        String string2 = "雅虎邮箱";
        byte[] bytes2 = string2.getBytes("UTF-8");

        String string3 = "";
        byte[] bytes3 = string3.getBytes("UTF-8");

        //arbitrary object
        ObjectId id = new ObjectId("myClass", new Id(1));
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] bytes4 = objectMapper.writeValueAsBytes(id);
        String string4 = "{\"className\":\"myClass\",\"id\":\"" + new Id(1).toStringForm() + "\"}";
        //null object

        byte[] bytes5 = ImmutableByteArray.NULL.getImmutableBytes();

        //null bytes
        byte[] bytes6 = null;

        //same as bytes5
        byte[] bytes7 = new byte[0];
        return new Object[][]{
            { bytes1, string1, bytes1.length },
            { bytes2, string2, bytes2.length },
            { bytes3, string3, bytes3.length },
            { bytes4, string4, bytes4.length },
            { bytes5, "", 0 },
            { bytes6, "", -1 },
            { bytes7, "", 0 }
        };
    }

    @DataProvider (name = "createMultiples")
    public static Object[][] createMultiples() {

        byte[] bytes1 = new byte[0];

        String string1 = "amazon";

        byte[] bytes2 = string1.getBytes();

        String string2 = "`";

        //    byte[] bytes3 = null;
        //    String string3 =  null;

        return new Object[][]{
            { bytes1, string1, bytes1, string1, string1.length() },
            { bytes1, string2, bytes1, string2, string2.length() },
            { bytes2, string1, bytes2, (string1 + string1 + string1), (string1 + string1 + string1).length() },
            { bytes2, string2, bytes2, (string1 + string2 + string1), (string1 + string2 + string1).length() }, //        {bytes1, string3, bytes1},
        //        {bytes2, string3, bytes2},
        //        {bytes3, string1, bytes3},
        //        {bytes3, string2, bytes3},
        //        {bytes3, string1, bytes2},
        };
    }

    @DataProvider (name = "createPair")
    public static Object[][] createPair() {

        String string1 = "amazon";
        String string2 = "雅虎邮箱";
        String string3 = "";
        String string4 = ImmutableByteArray.NULL.toString();
        String string5 = null;

        return new Object[][]{
            { string1, string1, true, 0 },
            { string2, string2, true, 0 },
            { string3, string3, true, 0 },
            { string4, string3, true, 0 },
            { string2, string1, false, 1 },
            { string3, string1, false, -1 }
        };
    }
}

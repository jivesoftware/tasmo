package com.jivesoftware.os.tasmo.id;

import org.testng.annotations.DataProvider;

public class TenantIdTestDataProvider {

    @DataProvider(name = "createStandardIdData")
    public static Object[][] createStandardIdData() {
        String id1 = "This is my TenantId";
        String id2 = "This is MY TenantId";

        return new Object[][] {
            { id1, id1, true },
            { id1, id2, false }
        };

    }

    @DataProvider(name = "createStandardTenantIdData")
    public static Object[][] createStandardTenantIdData() {
        String id1 = "This is my TenantId";

        TenantId t1 = new TenantId(id1);

        return new Object[][] {
            { t1, t1, true },
            { t1, null, false },
            { null, null, true },

        };

    }
}

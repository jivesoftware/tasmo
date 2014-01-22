package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TenantIdTest {

    @Test(dataProviderClass = TenantIdTestDataProvider.class, dataProvider = "createStandardIdData")
    public void testIdEquals(String id1, String id2, boolean expectedResult) throws Exception {

        TenantId t1 = new TenantId(id1);
        TenantId t2 = new TenantId(id2);

        assertEquals(expectedResult, t1.equals(t2));
        if (expectedResult) {
            assertEquals(t1.hashCode(), t2.hashCode(), "hashcode should be equal too");
        }

        assertEquals(id1, t1.toStringForm(), "should have the same tenantId");
        assertEquals(id2, t2.toStringForm(), "should have the same tenantId");

    }

    @Test(dataProviderClass = TenantIdTestDataProvider.class, dataProvider = "createStandardTenantIdData")
    public void testTenantIdEquals(TenantId t1, TenantId t2, boolean expectedResult) throws Exception {

        if (t1 != null) {
            assertEquals(t1.equals(t2), expectedResult);
        } else if (t2 != null) {
            assertEquals(t2.equals(t1), expectedResult);
        }
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        TenantId tenantId = new TenantId("foobar");
        String tenantIdJson = objectMapper.writeValueAsString(tenantId);
        TenantId tenantIdFromJson = objectMapper.readValue(tenantIdJson, TenantId.class);
        assertEquals(tenantIdFromJson, tenantId);
    }
}

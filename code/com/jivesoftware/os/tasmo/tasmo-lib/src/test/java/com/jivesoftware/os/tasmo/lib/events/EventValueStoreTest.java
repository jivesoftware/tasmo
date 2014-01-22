/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.lib.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore.Transaction;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class EventValueStoreTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private EventValueStore eventValueStore;
    private WrittenEventProvider<ObjectNode, JsonNode> eventProvider = new JsonWrittenEventProvider();

    @BeforeTest
    public void setUp() {
        EventValueCacheProvider cacheProvider = new EventValueCacheProvider() {
            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> createValueStoreCache() {
               return new RowColumnValueStoreImpl<>();
            }
        };

        eventValueStore = new EventValueStore(new RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue>(), cacheProvider);
    }

    /**
     * Test of get_aId method, of class ReferenceStore.
     */
    @Test
    public void testAdd() {
        TenantId tenantId = new TenantId("tenantId");
        Id userId = Id.NULL;
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        ObjectId objectId1 = new ObjectId("foo", new Id(1));
        ObjectId objectId2 = new ObjectId("foo", new Id(2));

        Transaction transaction = eventValueStore.begin(tenantIdAndCentricId, 2, 2, objectId1);
        transaction.set("bar", fieldVal("bar"));
        transaction.set("bazz", fieldVal("bazz"));
        eventValueStore.commit(transaction);

        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId1, new String[]{"bar"})[0].getValue(), fieldVal("bar"));
        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId1, new String[]{"bazz"})[0].getValue(), fieldVal("bazz"));

        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = eventValueStore.get(tenantIdAndCentricId, objectId1, new String[]{"bar", "bazz"});
        Assert.assertEquals(got[0].getValue(), fieldVal("bar"));
        Assert.assertEquals(got[1].getValue(), fieldVal("bazz"));

        transaction = eventValueStore.begin(tenantIdAndCentricId, 3, 3, objectId1);
        transaction.remove("bar");
        eventValueStore.commit(transaction);

        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId1, new String[]{"bar"})[0], null);

        transaction = eventValueStore.begin(tenantIdAndCentricId, 4, 4, objectId2);
        transaction.set("red", fieldVal("red"));
        transaction.set("blue", fieldVal("blue"));
        eventValueStore.commit(transaction);

        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId2, new String[]{"red"})[0].getValue(), fieldVal("red"));
        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId2, new String[]{"blue"})[0].getValue(), fieldVal("blue"));

        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId2, new String[]{"bar"})[0], null);
        Assert.assertEquals(eventValueStore.get(tenantIdAndCentricId, objectId2, new String[]{"bazz"})[0], null);

    }

    private OpaqueFieldValue fieldVal(Object value) {
        return eventProvider.convertFieldValue(mapper.convertValue(value, JsonNode.class));
    }
}

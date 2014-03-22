/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class ReferenceStoreTest {

    ConcurrencyStore concurrencyStore;
    ReferenceStore referenceStore;

    @BeforeTest
    public void setUp() {
        RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> updated = new RowColumnValueStoreImpl<>();
        concurrencyStore = new ConcurrencyStore(updated);

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        referenceStore = new ReferenceStore(concurrencyStore, multiLinks, multiBackLinks);
    }

    @Test(enabled = false) // TODO fix :)
    public void testMultiRefStoreAndRemoveLinks() throws Exception {
        AtomicInteger order = new AtomicInteger();
        TenantId tenantId = new TenantId("booya");
        Id userId = Id.NULL;
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        String aClassName = "Content";
        String aFieldName = "tagsA";
        Reference aId = new Reference(new ObjectId(aClassName, new Id(order.incrementAndGet())), "foo");
        Reference bId1 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        Reference bId2 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        Reference bId3 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        List<Reference> bIds = Arrays.asList(bId1, bId2, bId3);
        List<ReferenceWithTimestamp> versionResults = new ArrayList<>();
        long eventId = order.incrementAndGet();
        for (Reference bId : bIds) {
            versionResults.add(new ReferenceWithTimestamp(bId.getObjectId(), bId.getFieldName(), 0));
        }

        referenceStore.unlink(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, new ObjectIdResults());
        referenceStore.link(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, bIds);

        ObjectIdResults results = new ObjectIdResults();
        Set<String> aClassNames = Sets.newHashSet(aClassName);

        referenceStore.streamBackRefs(tenantIdAndCentricId, bId3.getObjectId(), aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Arrays.asList(new ReferenceWithTimestamp(aId.getObjectId(), aId.getFieldName(), 0))));
        results.results.clear();

        referenceStore.streamForwardRefs(tenantIdAndCentricId, aClassName, aFieldName, aId.getObjectId(), results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, versionResults));
        results.results.clear();

        bIds = Arrays.asList(bId1, bId3);
        eventId = order.incrementAndGet();
        versionResults.clear();
        for (Reference bId : bIds) {
            versionResults.add(new ReferenceWithTimestamp(bId.getObjectId(), bId.getFieldName(), 0));
        }
        referenceStore.unlink(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, new ObjectIdResults());
        referenceStore.link(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, bIds);

        referenceStore.streamBackRefs(tenantIdAndCentricId, bId3.getObjectId(), aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Arrays.asList(new ReferenceWithTimestamp(aId.getObjectId(), aId.getFieldName(), 0))));
        results.results.clear();

        //bid2 is no longer linked to by aId
        referenceStore.streamBackRefs(tenantIdAndCentricId, bId2.getObjectId(), aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Collections.<ReferenceWithTimestamp>emptyList()));
        results.results.clear();

        referenceStore.streamForwardRefs(tenantIdAndCentricId, aClassName, aFieldName, aId.getObjectId(), results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, versionResults));
        results.results.clear();

        bIds = Collections.emptyList();
        eventId = order.incrementAndGet();
        versionResults.clear();
        for (Reference bId : bIds) {
            versionResults.add(new ReferenceWithTimestamp(bId.getObjectId(), bId.getFieldName(), 0));
        }
        referenceStore.unlink(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, new ObjectIdResults());
        referenceStore.link(tenantIdAndCentricId, eventId, aId.getObjectId(), aFieldName, bIds);

        //bid1 is no longer linked to by aId
        referenceStore.streamBackRefs(tenantIdAndCentricId, bId1.getObjectId(), aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Collections.<ReferenceWithTimestamp>emptyList()));
        results.results.clear();

        referenceStore.streamForwardRefs(tenantIdAndCentricId, aClassName, aFieldName, aId.getObjectId(), results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, Collections.<ReferenceWithTimestamp>emptyList()));
        results.results.clear();
    }

    public boolean equal(List<ReferenceWithTimestamp> a, List<ReferenceWithTimestamp> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).getObjectId().equals(b.get(i).getObjectId())) {
                return false;
            }
        }
        return true;
    }

    private static class ObjectIdResults implements CallbackStream<ReferenceWithTimestamp> {

        final List<ReferenceWithTimestamp> results = Lists.newArrayList();

        @Override
        public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
            if (v != null) {
                results.add(v);
            }
            return v;
        }
    }
}

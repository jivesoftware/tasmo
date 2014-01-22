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

    ReferenceStore referenceStore;

    @BeforeTest
    public void setUp() {
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        referenceStore = new ReferenceStore(multiLinks, multiBackLinks);
    }

    @Test
    public void testMultiRefStoreAndRemoveLinks() throws Exception {
        AtomicInteger order = new AtomicInteger();
        TenantId tenantId = new TenantId("booya");
        Id userId = Id.NULL;
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        String aClassName = "Content";
        String aFieldName = "tags";
        Reference aId = new Reference(new ObjectId(aClassName, new Id(order.incrementAndGet())), 0);
        Reference bId1 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), 0);
        Reference bId2 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), 0);
        Reference bId3 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), 0);
        List<Reference> bIds = Arrays.asList(bId1, bId2, bId3);
        List<Reference> versionResults = new ArrayList<>();
        long eventId = order.incrementAndGet();
        for (Reference bId : bIds) {
            versionResults.add(bId);
        }

        referenceStore.remove_aId_aField(tenantIdAndCentricId, eventId, aId, aFieldName, new ObjectIdResults());
        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, eventId, aId, aFieldName, bIds);

        ObjectIdResults results = new ObjectIdResults();
        Set<String> aClassNames = Sets.newHashSet(aClassName);

        referenceStore.get_aIds(tenantIdAndCentricId, bId3, aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Arrays.asList(aId)));
        results.results.clear();

        referenceStore.get_bIds(tenantIdAndCentricId, aClassName, aFieldName, aId, results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, versionResults));
        results.results.clear();

        bIds = Arrays.asList(bId1, bId3);
        eventId = order.incrementAndGet();
        versionResults.clear();
        for (Reference bId : bIds) {
            versionResults.add(bId);
        }
        referenceStore.remove_aId_aField(tenantIdAndCentricId, eventId, aId, aFieldName, new ObjectIdResults());
        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, eventId, aId, aFieldName, bIds);

        referenceStore.get_aIds(tenantIdAndCentricId, bId3, aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Arrays.asList(aId)));
        results.results.clear();

        //bid2 is no longer linked to by aId
        referenceStore.get_aIds(tenantIdAndCentricId, bId2, aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Collections.<Reference>emptyList()));
        results.results.clear();

        referenceStore.get_bIds(tenantIdAndCentricId, aClassName, aFieldName, aId, results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, versionResults));
        results.results.clear();

        bIds = Collections.emptyList();
        eventId = order.incrementAndGet();
        versionResults.clear();
        for (Reference bId : bIds) {
            versionResults.add(bId);
        }
        referenceStore.remove_aId_aField(tenantIdAndCentricId, eventId, aId, aFieldName, new ObjectIdResults());
        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, eventId, aId, aFieldName, bIds);

        //bid1 is no longer linked to by aId
        referenceStore.get_aIds(tenantIdAndCentricId, bId1, aClassNames, aFieldName, results);
        Assert.assertTrue(equal(results.results, Collections.<Reference>emptyList()));
        results.results.clear();

        referenceStore.get_bIds(tenantIdAndCentricId, aClassName, aFieldName, aId, results);
        //in memory reference store will return ascending sorted lists
        Assert.assertTrue(equal(results.results, Collections.<Reference>emptyList()));
        results.results.clear();
    }

    public boolean equal(List<Reference> a, List<Reference> b) {
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

    private static class ObjectIdResults implements CallbackStream<Reference> {

        final List<Reference> results = Lists.newArrayList();

        @Override
        public Reference callback(Reference v) throws Exception {
            if (v != null) {
                results.add(v);
            }
            return v;
        }
    }
}

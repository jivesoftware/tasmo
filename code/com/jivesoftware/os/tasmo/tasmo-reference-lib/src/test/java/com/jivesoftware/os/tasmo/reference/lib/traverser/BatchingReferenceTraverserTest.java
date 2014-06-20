package com.jivesoftware.os.tasmo.reference.lib.traverser;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class BatchingReferenceTraverserTest {

    private final TenantId tenantId = new TenantId("booya");
    private final Id userId = Id.NULL;
    private final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
    private final String aClassName = "Content";
    private final String aFieldName = "tagsA";

    ConcurrencyStore concurrencyStore;
    ReferenceStore referenceStore;

    @BeforeTest
    public void setUp() {
        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updated = new RowColumnValueStoreImpl<>();
        concurrencyStore = new ConcurrencyStore(updated);

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        referenceStore = new ReferenceStore(concurrencyStore, multiLinks, multiBackLinks);
    }

    /**
     * Test of processRequests method, of class BatchingReferenceTraverser.
     */
    @Test
    public void basicTest() throws Exception {
        AtomicInteger order = new AtomicInteger();
        Reference aId = new Reference(new ObjectId(aClassName, new Id(order.incrementAndGet())), "foo");
        Reference bId1 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        Reference bId2 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        Reference bId3 = new Reference(new ObjectId("Tag", new Id(order.incrementAndGet())), "foo");
        List<Reference> bIds = Arrays.asList(bId1, bId2, bId3);

        long eventId = order.incrementAndGet();
        referenceStore.link(tenantIdAndCentricId, aId.getObjectId(), eventId, Arrays.asList(new ReferenceStore.LinkTo(aFieldName, bIds)));

        ListeningExecutorService traverserExecutors = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));
        final BatchingReferenceTraverser instance = new BatchingReferenceTraverser(referenceStore, traverserExecutors, 10, 10000);
        final AtomicBoolean running = new AtomicBoolean(true);
        Executors.newSingleThreadExecutor().submit(new Runnable() {

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        instance.startProcessingRequests();
                        Thread.sleep(10);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                }
            }

        });

        final List<ReferenceWithTimestamp> got = new ArrayList<>();
        System.out.println("A");
        instance.traverseForwardRef(tenantIdAndCentricId, Collections.singleton(aClassName), aFieldName, aId.getObjectId(), order.incrementAndGet(),
                new CallbackStream<ReferenceWithTimestamp>() {

                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                        if (v != null) {
                            System.out.println("v:" + v);
                            got.add(v);
                        }
                        return v;
                    }
                });
        System.out.println(got);
        Assert.assertEquals(3, got.size());

        got.clear();
        instance.traversBackRefs(tenantIdAndCentricId, Collections.singleton(aClassName), aFieldName, bId1.getObjectId(), order.incrementAndGet(),
                new CallbackStream<ReferenceWithTimestamp>() {

                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                        if (v != null) {
                            System.out.println("v:" + v);
                            got.add(v);
                        }
                        return v;
                    }
                });
        System.out.println(got);
        Assert.assertEquals(1, got.size());

        running.set(false);
    }

}

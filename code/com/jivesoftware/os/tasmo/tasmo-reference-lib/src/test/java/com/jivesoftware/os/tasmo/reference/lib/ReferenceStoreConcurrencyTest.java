/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantIdAndRow;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.FieldVersion;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathConsistencyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class ReferenceStoreConcurrencyTest {

    @Test(invocationCount = 1, singleThreaded = false, skipFailedInvocations = true)
    public void testConcurrencyMultiRefStore() throws Exception {

        //System.out.println("\n |--> BEGIN \n");
        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updated = new RowColumnValueStoreImpl<>();
        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(updated);

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, multiLinks, multiBackLinks);

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        final TenantId tenantId = new TenantId("booya");
        Id userId = Id.NULL;
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);

        ObjectId from = new ObjectId("A", new Id(rand.nextInt(1_000)));
        String fromRefFieldName = "fromRefFieldName";

        final RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values = new RowColumnValueStoreImpl<>();

        int eventCount = 50;
        Event[] events = new Event[eventCount];
        for (int i = 0; i < eventCount; i++) {
            int time = i * 2;
            if (rand.nextBoolean()) {
                events[i] = new Event(concurrencyStore, referenceStore, values, tenantIdAndCentricId, time, true, from, fromRefFieldName, new Reference[0], -1);
            } else {
                Reference[] tos = new Reference[1 + rand.nextInt(10)];
                for (int j = 0; j < tos.length; j++) {
                    tos[j] = new Reference(new ObjectId("B", new Id(rand.nextInt(5))), fromRefFieldName);
                }
                events[i] = new Event(concurrencyStore, referenceStore, values,
                        tenantIdAndCentricId, time, false, from, fromRefFieldName, tos, Math.abs(rand.nextLong()));
            }
        }
        final Event winner = events[events.length - 1];

        List<Event> shuffled = Arrays.asList(events);
        Collections.shuffle(shuffled);

        ExecutorService executor = Executors.newFixedThreadPool(128);
        final CountDownLatch latch = new CountDownLatch(eventCount);
        for (final Event e : shuffled) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        //System.out.println(Thread.currentThread()+" |--> event "+e);
                        e.process();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        executor.shutdownNow();
        //Thread.sleep(1000);
        //System.out.println("------------------------------------------------");

        System.out.println("Is:");
        final AtomicInteger failures = new AtomicInteger();
        values.getAllRowKeys(100, null, new CallbackStream<TenantIdAndRow<TenantIdAndCentricId, ObjectId>>() {

            @Override
            public TenantIdAndRow<TenantIdAndCentricId, ObjectId> callback(
                    final TenantIdAndRow<TenantIdAndCentricId, ObjectId> row) throws Exception {
                if (row != null) {
                    values.getEntrys(row.getTenantId(), row.getRow(), null, Long.MAX_VALUE, 1_000, false, null, null,
                            new CallbackStream<ColumnValueAndTimestamp<String, Long, Long>>() {

                                @Override
                                public ColumnValueAndTimestamp<String, Long, Long> callback(ColumnValueAndTimestamp<String, Long, Long> v) throws Exception {
                                    if (v != null) {

                                        System.out.println(" |--> " + row.getRow()
                                                + " | " + v.getColumn()
                                                + " | " + v.getValue() + " | " + v.getTimestamp());
                                        if (!winner.contains(row.getRow(), v.getTimestamp())) {
                                            failures.incrementAndGet();
                                        }
                                    }
                                    return v;
                                }
                            }
                    );
                }
                return row;
            }
        });

        System.out.println("Want:");
        System.out.println(winner);

        System.out.println("Seed:" + seed);

        boolean passed = failures.get() == 0;
        if (passed) {
            System.out.println("PASSED");
        } else {
            System.out.println("FAILED");
            //System.out.println(" |--> FAILED");
            //System.exit(0);
        }

        Assert.assertTrue(passed, "Seed:" + seed + " Failed:" + failures.get());
    }

    static class Event {

        ConcurrencyStore concurrencyStore;
        ReferenceStore referenceStore;
        RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values;
        TenantIdAndCentricId tenantIdAndCentricId;
        long timestamp;
        boolean delete;
        ObjectId from;
        String fromRefFieldName;
        Reference[] tos;
        long value;

        Event(ConcurrencyStore concurrencyStore,
                ReferenceStore referenceStore,
                RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values,
                TenantIdAndCentricId tenantIdAndCentricId,
                long timestamp,
                boolean delete,
                ObjectId from,
                String fromRefFieldName,
                Reference[] tos,
                long value) {
            this.concurrencyStore = concurrencyStore;
            this.referenceStore = referenceStore;
            this.values = values;
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.timestamp = timestamp;
            this.delete = delete;
            this.from = from;
            this.fromRefFieldName = fromRefFieldName;
            this.tos = tos;
            this.value = value;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (delete) {
                sb.append("deleted at timestamp:").append(timestamp).append("\n");
            } else {
                List<String> rows = new ArrayList<>();
                for (Reference t : tos) {
                    StringBuilder row = new StringBuilder();
                    row.append(t.getObjectId()).append(" | ")
                            .append(t.getFieldName()).append(" | ").append(value).append(" | ").append(timestamp).append("\n");
                    rows.add(row.toString());
                }
                Collections.sort(rows);
                for (String r : rows) {
                    sb.append(" |--> ").append(r);
                }
            }
            return sb.toString();
        }

        void process() {
            int attempts = 0;
            int maxAttempts = 1_000;
            while (attempts < maxAttempts) {
                attempts++;
                if (attempts > 1) {
                    //System.out.println(Thread.currentThread() + " attempts " + attempts);
                }
                try {

                    if (delete) {
                        referenceStore.unlink(tenantIdAndCentricId, timestamp, from, fromRefFieldName, 0,
                                new CallbackStream<ReferenceWithTimestamp>() {
                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            values.remove(tenantIdAndCentricId,
                                                    v.getObjectId(), fromRefFieldName, new ConstantTimestamper(v.getTimestamp() + 1));
                                        }
                                        return v;
                                    }
                                });
                    } else {
                        final long highest = concurrencyStore.highest(tenantIdAndCentricId, from, fromRefFieldName, timestamp);
                        if (timestamp >= highest) {
                            referenceStore.link(tenantIdAndCentricId, from, timestamp,
                                    Arrays.asList(new ReferenceStore.LinkTo(fromRefFieldName, Arrays.asList(tos))));
                        }
                        // yield
                        referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), from, fromRefFieldName, 0,
                                new CallbackStream<ReferenceWithTimestamp>() {
                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            values.remove(tenantIdAndCentricId,
                                                    v.getObjectId(), fromRefFieldName, new ConstantTimestamper(v.getTimestamp() + 1));
                                        }
                                        return v;
                                    }
                                });

                        final Set<FieldVersion> want = new HashSet<>(Arrays.asList(new FieldVersion(from, fromRefFieldName, highest)));
                        Set<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
                        if (got != want) {
                            PathConsistencyException e = new PathConsistencyException(want, got);
                            throw e;
                        }

                        final List<Add> adds = new ArrayList<>();
                        referenceStore.streamForwardRefs(tenantIdAndCentricId, Collections.singleton(from.getClassName()), fromRefFieldName, from, 0,
                                new CallbackStream<ReferenceWithTimestamp>() {

                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            want.add(new FieldVersion(from, fromRefFieldName, v.getTimestamp()));
                                            adds.add(new Add(tenantIdAndCentricId, v.getObjectId(), fromRefFieldName, value, v.getTimestamp()));

                                        //Set<FieldVersion> want = Collections.singleton(new FieldVersion(from, fromRefFieldName, v.getTimestamp()));
                                            //Set<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
                                            //if (got == want) {
                                            //    values.add(tenantIdAndCentricId, v.getObjectId(), fromRefFieldName, value, null,
                                            //        new ConstantTimestamper(v.getTimestamp()));
                                            //    System.out.println(Thread.currentThread().getName()+" |--> add "+v.getObjectId()+" "+v.getTimestamp());
                                            //} else {
                                            //    PathConsistencyException e = new PathConsistencyException(want, got);
                                            //    System.out.println(Thread.currentThread() + " " + e.toString());
                                            //    throw e;
                                            //}
                                        }
                                        return v;
                                    }
                                });

                        got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
                        if (got != want) {
                            PathConsistencyException e = new PathConsistencyException(want, got);
                            //System.out.println(Thread.currentThread() + " " + e.toString());
                            throw e;
                        }
                        for (Add add : adds) {
                            values.add(add.tenantId, add.rowKey, add.columnKey, add.columnValue, null, new ConstantTimestamper(add.timestamp));
                        }
                    }
                    return;

                } catch (Exception e) {
                    boolean pathModifiedException = false;
                    Throwable t = e;
                    while (t != null) {
                        if (t instanceof PathConsistencyException) {
                            pathModifiedException = true;
                            //System.out.println(t.toString());
                        }
                        t = t.getCause();
                    }
                    if (pathModifiedException) {
                        try {
                            //System.out.println(e.toString());
                            Thread.sleep(1); // Yield a better choice?
                        } catch (InterruptedException ex) {
                        }
                    } else {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
            if (attempts >= maxAttempts) {
                throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts. " + this);
            }
        }

        private boolean contains(ObjectId id, Long timestamp) {
            if (this.timestamp != timestamp) {
                System.out.println(" BUMMER: failed to find:" + timestamp + " had:" + this.timestamp);
                return false;
            }
            for (Reference t : tos) {
                if (t.getObjectId().equals(id)) {
                    return true;
                }
            }
            System.out.println(" BUMMER: failed to find " + id + " " + timestamp + " " + this);
            return false;
        }
    }

    static class Add {

        TenantIdAndCentricId tenantId;
        ObjectId rowKey;
        String columnKey;
        Long columnValue;
        long timestamp;

        Add(TenantIdAndCentricId tenantId, ObjectId rowKey, String columnKey, Long columnValue, long timestamp) {
            this.tenantId = tenantId;
            this.rowKey = rowKey;
            this.columnKey = columnKey;
            this.columnValue = columnValue;
            this.timestamp = timestamp;
        }
    }
}

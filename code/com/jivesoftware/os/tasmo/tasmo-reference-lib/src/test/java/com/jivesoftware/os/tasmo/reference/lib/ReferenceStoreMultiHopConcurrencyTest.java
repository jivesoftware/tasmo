/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantIdAndRow;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class ReferenceStoreMultiHopConcurrencyTest {

    @Test (invocationCount = 1, singleThreaded = false, skipFailedInvocations = true)
    public void testConcurrencyMultiRefStore() throws Exception {

        //System.out.println("\n |--> BEGIN \n");
        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updated = new RowColumnValueStoreImpl<>();
        ConcurrencyStore concurrencyStore = new ConcurrencyStore(updated);

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, multiLinks, multiBackLinks);

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        final TenantId tenantId1 = new TenantId("booya");
        Id userId = Id.NULL;
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId1, userId);

        ObjectId fromA = new ObjectId("A", new Id(rand.nextInt(1000)));

        final RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values = new RowColumnValueStoreImpl<>();

        int eventCount = 50;
        List<Event> events = new ArrayList<>();
        Event lastARefBEvent = null;
        Event lastBRefCEvent = null;
        int t = 0;
        for (int i = 0; i < eventCount; i++) {
            if (rand.nextBoolean()) {
                lastARefBEvent = new Event(true, concurrencyStore, referenceStore, values,
                    tenantIdAndCentricId, t, true, fromA, new Reference[0], -1);
                events.add(lastARefBEvent);
                t+=2;
            } else {
                Reference[] toBs = new Reference[1 + rand.nextInt(10)];
                for (int j = 0; j < toBs.length; j++) {
                    ObjectId fromB = new ObjectId("B", new Id(rand.nextInt(10)));
                    toBs[j] = new Reference(fromB, "aToB");

                    for (int ii = 0; ii < eventCount; ii++) {
                       if (rand.nextBoolean()) {
                           lastBRefCEvent = new Event(false, concurrencyStore, referenceStore, values,
                               tenantIdAndCentricId, t, true, fromB, new Reference[0], -1);
                           events.add(lastBRefCEvent);
                           t+=2;
                       } else {
                           Reference[] toCs = new Reference[1 + rand.nextInt(10)];
                           for (int jj = 0; jj < toCs.length; jj++) {
                               toCs[jj] = new Reference(new ObjectId("C", new Id(rand.nextInt(10))), "bToC");
                           }
                           lastBRefCEvent = new Event(false, concurrencyStore, referenceStore, values,
                               tenantIdAndCentricId, t, false, fromB, toCs, Math.abs(rand.nextLong()));
                           events.add(lastBRefCEvent);
                           t+=2;
                       }
                    }

                }
                lastARefBEvent = new Event(true, concurrencyStore, referenceStore, values,
                    tenantIdAndCentricId, t, false, fromA, toBs, Math.abs(rand.nextLong()));
                events.add(lastARefBEvent);
                t+=2;
            }
        }
        final Event winner = lastARefBEvent;
        Collections.shuffle(events);

        ExecutorService executor = Executors.newFixedThreadPool(128);
        final CountDownLatch latch = new CountDownLatch(eventCount);
        for (final Event e : events) {
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
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        //Thread.sleep(1000);
        //System.out.println("------------------------------------------------");


        System.out.println("Is:");
        final AtomicInteger failures = new AtomicInteger();
        values.getAllRowKeys(100, null, new CallbackStream<TenantIdAndRow<TenantIdAndCentricId, ObjectId>>() {

            @Override
            public TenantIdAndRow<TenantIdAndCentricId, ObjectId> callback(
                final TenantIdAndRow<TenantIdAndCentricId, ObjectId> row) throws Exception {
                if (row != null) {
                    values.getEntrys(row.getTenantId(), row.getRow(), null, Long.MAX_VALUE, 1000, false, null, null,
                        new CallbackStream<ColumnValueAndTimestamp<String, Long, Long>>() {

                            @Override
                            public ColumnValueAndTimestamp<String, Long, Long> callback(ColumnValueAndTimestamp<String, Long, Long> v) throws Exception {
                                if (v != null) {
                                    if (v.getColumn().equals("aToB")) {

                                        System.out.println(" |--> " + row.getRow()
                                            + " | " + v.getColumn()
                                            + " | " + v.getValue() + " | " + v.getTimestamp());
                                        if (!winner.contains(row.getRow(), v.getTimestamp())) {
                                            failures.incrementAndGet();
                                        }
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

        boolean firstHop;
        ConcurrencyStore concurrencyStore;
        ReferenceStore referenceStore;
        RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values;
        TenantIdAndCentricId tenantIdAndCentricId;
        long timestamp;
        boolean delete;
        ObjectId from;
        Reference[] tos;
        long value;

        public Event(boolean firstHop,
            ConcurrencyStore concurrencyStore,
            ReferenceStore referenceStore,
            RowColumnValueStoreImpl<TenantIdAndCentricId, ObjectId, String, Long> values,
            TenantIdAndCentricId tenantIdAndCentricId,
            long timestamp,
            boolean delete,
            ObjectId from,
            Reference[] tos,
            long value) {
            this.firstHop = firstHop;
            this.concurrencyStore = concurrencyStore;
            this.referenceStore = referenceStore;
            this.values = values;
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.timestamp = timestamp;
            this.delete = delete;
            this.from = from;
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
            int maxAttempts = 1000;
            while (attempts < maxAttempts) {
                attempts++;
                if (attempts > 1) {
                    //System.out.println(Thread.currentThread() + " attempts " + attempts);
                }
                try {

                    if (delete) {
                        if (firstHop) {
                            referenceStore.unlink(tenantIdAndCentricId, timestamp, from, "aToB", 0,
                                new CallbackStream<ReferenceWithTimestamp>() {
                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            values.remove(tenantIdAndCentricId,
                                                v.getObjectId(), "aToB", new ConstantTimestamper(v.getTimestamp() + 1));
                                        }
                                        return v;
                                    }
                                });
                        } else {

                        }
                    } else {
                        if (firstHop) {
                            final long highest = concurrencyStore.highest(tenantIdAndCentricId, from, "aToB", timestamp);
                            if (timestamp >= highest) {
                                referenceStore.link(tenantIdAndCentricId, from, timestamp,
                                    Arrays.asList(new ReferenceStore.LinkTo("aToB", Arrays.asList(tos))));
                            }
                            // yield
                            referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), from, "aToB", 0,
                                new CallbackStream<ReferenceWithTimestamp>() {
                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            values.remove(tenantIdAndCentricId,
                                                v.getObjectId(), "aToB", new ConstantTimestamper(v.getTimestamp() + 1));
                                        }
                                        return v;
                                    }
                                });

                            final Set<FieldVersion> want = new HashSet<>(Arrays.asList(new FieldVersion(from, "aToB", highest)));
                            Set<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
                            if (got != want) {
                                PathConsistencyException e = new PathConsistencyException(want, got);
                                throw e;
                            }

                            final List<Add> adds = new ArrayList<>();
                            referenceStore.streamForwardRefs(tenantIdAndCentricId, Collections.singleton(from.getClassName()), "aToB", from, 0,
                                new CallbackStream<ReferenceWithTimestamp>() {

                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            want.add(new FieldVersion(from, "aToB", v.getTimestamp()));
                                            adds.add(new Add(tenantIdAndCentricId, v.getObjectId(), "aToB", value, v.getTimestamp()));


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
                            for(Add add:adds) {
                                values.add(add.tenantId, add.rowKey, add.columnKey, add.columnValue, null, new ConstantTimestamper(add.timestamp));
                            }
                        } else {

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
                            Thread.sleep(1); // TODO is yield a better choice?
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

        public Add(TenantIdAndCentricId tenantId, ObjectId rowKey, String columnKey, Long columnValue, long timestamp) {
            this.tenantId = tenantId;
            this.rowKey = rowKey;
            this.columnKey = columnKey;
            this.columnValue = columnValue;
            this.timestamp = timestamp;
        }
    }
}

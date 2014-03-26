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
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author pete
 */
public class ReferenceStoreConcurrencyTest {

    @Test(invocationCount = 500, singleThreaded = false)
    public void testConcurrencyMultiRefStore() throws Exception {

        //System.out.println("\n |--> BEGIN \n");
        RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> updated = new RowColumnValueStoreImpl<>();
        ConcurrencyStore concurrencyStore = new ConcurrencyStore(updated);

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, multiLinks, multiBackLinks);

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        final TenantId tenantId = new TenantId("booya");
        Id userId = Id.NULL;
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);

        ObjectId from = new ObjectId("A", new Id(rand.nextInt(1000)));
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
                    tos[j] = new Reference(new ObjectId("B", new Id(rand.nextInt(1000))), fromRefFieldName);
                }
                events[i] = new Event(concurrencyStore, referenceStore, values,
                        tenantIdAndCentricId, time, false, from, fromRefFieldName, tos, Math.abs(rand.nextLong()));
            }
        }

        Executor executor = Executors.newCachedThreadPool();
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
        //Thread.sleep(1000);
        //System.out.println("------------------------------------------------");

        final Event winner = events[events.length - 1];
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

        public Event(ConcurrencyStore concurrencyStore,
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
            int maxAttempts = 1000;
            while (attempts < maxAttempts) {
                attempts++;
                if (attempts > 1) {
                    //System.out.println(Thread.currentThread() + " attempts " + attempts);
                }
                try {

                    if (delete) {

                        referenceStore.unlink(tenantIdAndCentricId, timestamp, from, fromRefFieldName,
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
                        final long highest = concurrencyStore.highest(tenantIdAndCentricId.getTenantId(), from, fromRefFieldName, timestamp);
                        if (timestamp >= highest) {
                            referenceStore.link(tenantIdAndCentricId, timestamp, from, fromRefFieldName, Arrays.asList(tos));
                        }
                        // yield
                        referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), from, fromRefFieldName,
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

                        List<FieldVersion> want = Arrays.asList(new FieldVersion(from, fromRefFieldName, highest));
                        List<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId.getTenantId(), want);
                        if (got != want) {
                            PathModifiedOutFromUnderneathMeException e = new PathModifiedOutFromUnderneathMeException(want, got);
                            throw e;
                        }

                        referenceStore.streamForwardRefs(tenantIdAndCentricId, from.getClassName(), fromRefFieldName, from,
                                new CallbackStream<ReferenceWithTimestamp>() {

                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp v) throws Exception {
                                        if (v != null) {
                                            List<FieldVersion> want = Arrays.asList(new FieldVersion(from, fromRefFieldName, v.getTimestamp()));
                                            List<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId.getTenantId(), want);
                                            if (got == want) {
                                                values.add(tenantIdAndCentricId, v.getObjectId(), fromRefFieldName, value, null,
                                                        new ConstantTimestamper(v.getTimestamp()));
                                                //System.out.println(Thread.currentThread().getName()+" |--> add "+v.getObjectId()+" "+v.getTimestamp());
                                            } else {
                                                PathModifiedOutFromUnderneathMeException e = new PathModifiedOutFromUnderneathMeException(want, got);
                                                //System.out.println(Thread.currentThread() + " " + e.toString());
                                                throw e;
                                            }
                                        }
                                        return v;
                                    }
                                });

                        got = concurrencyStore.checkIfModified(tenantIdAndCentricId.getTenantId(), want);
                        if (got != want) {
                            PathModifiedOutFromUnderneathMeException e = new PathModifiedOutFromUnderneathMeException(want, got);
                            //System.out.println(Thread.currentThread() + " " + e.toString());
                            throw e;
                        }
                    }
                    return;

                } catch (Exception e) {
                    boolean pathModifiedException = false;
                    Throwable t = e;
                    while (t != null) {
                        if (t instanceof PathModifiedOutFromUnderneathMeException) {
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
                System.out.println(" FUCK ME: failed to find:" + timestamp + " had:" + this.timestamp);
                return false;
            }
            for (Reference t : tos) {
                if (t.getObjectId().equals(id)) {
                    return true;
                }
            }
            System.out.println(" FUCK ME: failed to find " + id + " " + timestamp + " " + this);
            return false;
        }
    }
}

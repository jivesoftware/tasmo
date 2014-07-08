package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdProvider;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class ConcurrencyTest extends BaseTest {

    public static final Random rand = new Random();

    @Test (dataProvider = "tasmoAsyncMaterializer", enabled = false, invocationCount = 10, singleThreaded = true, skipFailedInvocations = true)
    public void concurrencyTest(TasmoMaterializerHarness async, TasmoMaterializerHarness sync) throws Exception {

        // Folder->Doc->User
        String[] binding = new String[]{
            "ATest::1::A.x",
            "ATest::2::A.ref_toB.ref.B|B.i",
            "ATest::3::A.ref_toB.ref.B|B.j",
            "ATest::4::A.ref_toB.ref.B|B.ref_toC.ref.C|C.a,b,c",
            "ATest::5::A.ref_toB.ref.B|B.ref_toC.ref.C|C.ref_toD.ref.D|D.k,l,m",
            "CTest::5::C.backRefs.B.ref_toC|B.backRefs.A.ref_toB|A.x",
            "ATestDup::1::A.x",
            "ATestDup::2::A.ref_toB.ref.B|B.i",
            "ATestDup::3::A.ref_toB.ref.B|B.j",
            "ATestDup::4::A.ref_toB.ref.B|B.ref_toC.ref.C|C.a,b,c",
            "ATestDup::5::A.ref_toB.ref.B|B.ref_toC.ref.C|C.ref_toD.ref.D|D.k,l,m",
            "CTestDup::5::C.backRefs.B.ref_toC|B.backRefs.A.ref_toB|A.x"
        };

        Views views = TasmoModelFactory.modelToViews(binding);
        async.initModel(views);
        sync.initModel(views);

        FireableValue a1 = new FireableValue(async, 1_000_000, "A", new String[]{ "x", "y", "z" }, new String[]{ "1", "2", "3" });
        FireableValue b1 = new FireableValue(async, 1_001_002, "B", new String[]{ "h", "i", "j" }, new String[]{ "1", "2", "3" });
        FireableValue b2 = new FireableValue(async, 1_002_002, "B", new String[]{ "h", "i", "j" }, new String[]{ "1", "2", "3" });
        FireableValue b3 = new FireableValue(async, 1_003_002, "B", new String[]{ "h", "i", "j" }, new String[]{ "1", "2", "3" });
        FireableValue c1 = new FireableValue(async, 1_001_004, "C", new String[]{ "a", "b", "c" }, new String[]{ "1", "2", "3" });
        FireableValue c2 = new FireableValue(async, 1_002_004, "C", new String[]{ "a", "b", "c" }, new String[]{ "1", "2", "3" });
        FireableValue c3 = new FireableValue(async, 1_003_004, "C", new String[]{ "a", "b", "c" }, new String[]{ "1", "2", "3" });
        FireableValue d1 = new FireableValue(async, 1_001_006, "D", new String[]{ "k", "l", "m" }, new String[]{ "1", "2", "3" });
        FireableValue d2 = new FireableValue(async, 1_002_006, "D", new String[]{ "k", "l", "m" }, new String[]{ "1", "2", "3" });
        FireableValue d3 = new FireableValue(async, 1_003_006, "D", new String[]{ "k", "l", "m" }, new String[]{ "1", "2", "3" });

        FireableRef refA = new FireableRef(async, a1, "toB", Arrays.asList(b1, b2, b3), b1);
        FireableRef refB1 = new FireableRef(async, b1, "toC", Arrays.asList(c1, c2, c3), c1);
        FireableRef refB2 = new FireableRef(async, b2, "toC", Arrays.asList(c1, c2, c3), c2);
        FireableRef refB3 = new FireableRef(async, b3, "toC", Arrays.asList(c1, c2, c3), c3);
        FireableRef refC1 = new FireableRef(async, c1, "toD", Arrays.asList(d1, d2, d3), d1);
        FireableRef refC2 = new FireableRef(async, c2, "toD", Arrays.asList(d1, d2, d3), d2);
        FireableRef refC3 = new FireableRef(async, c3, "toD", Arrays.asList(d1, d2, d3), d3);


        //ExecutorService threads = MoreExecutors.sameThreadExecutor();
        ExecutorService threads = Executors.newCachedThreadPool();
        List<RandomFireable<?>> fireables = new ArrayList<>();

        CountDownLatch latch = new CountDownLatch(30);
        fireables.add(new RandomFireable<>(a1, 1, 250, latch));
        fireables.add(new RandomFireable<>(a1, 1, 250, latch));
        fireables.add(new RandomFireable<>(b1, 1, 250, latch));
        fireables.add(new RandomFireable<>(b2, 1, 250, latch));
        fireables.add(new RandomFireable<>(b3, 1, 250, latch));
        fireables.add(new RandomFireable<>(b1, 1, 250, latch));
        fireables.add(new RandomFireable<>(b2, 1, 250, latch));
        fireables.add(new RandomFireable<>(b3, 1, 250, latch));
        fireables.add(new RandomFireable<>(c1, 1, 250, latch));
        fireables.add(new RandomFireable<>(c2, 1, 250, latch));
        fireables.add(new RandomFireable<>(c3, 1, 250, latch));
        fireables.add(new RandomFireable<>(c1, 1, 250, latch));
        fireables.add(new RandomFireable<>(c2, 1, 250, latch));
        fireables.add(new RandomFireable<>(c3, 1, 250, latch));
        fireables.add(new RandomFireable<>(d1, 1, 250, latch));
        fireables.add(new RandomFireable<>(d2, 1, 250, latch));
        fireables.add(new RandomFireable<>(d3, 1, 250, latch));
        fireables.add(new RandomFireable<>(d1, 1, 250, latch));
        fireables.add(new RandomFireable<>(d2, 1, 250, latch));
        fireables.add(new RandomFireable<>(d3, 1, 250, latch));
        fireables.add(new RandomFireable<>(refA, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refA, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB1, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB2, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB3, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB1, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB2, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refB3, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refC1, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refC2, 10, 2000, latch));
        fireables.add(new RandomFireable<>(refC3, 10, 2000, latch));

        List<Future<Void>> futures = threads.invokeAll(fireables);
        latch.await();

        threads.shutdown();
        threads.awaitTermination(60, TimeUnit.SECONDS);

        for (Future<Void> future : futures) {
            future.get();
        }


        // Yes this sucks!
        a1.t  = sync; a1.finalEvent();
        b1.t  = sync; b1.finalEvent();
        b2.t  = sync; b2.finalEvent();
        b3.t  = sync; b3.finalEvent();
        c1.t  = sync; c1.finalEvent();
        c2.t  = sync; c2.finalEvent();
        c3.t  = sync; c3.finalEvent();
        d1.t  = sync; d1.finalEvent();
        d2.t  = sync; d2.finalEvent();
        d3.t  = sync; d3.finalEvent();


        refA.t  = sync; refA.finalEvent();
        refB1.t  = sync; refB1.finalEvent();
        refB2.t  = sync; refB2.finalEvent();
        refB3.t  = sync; refB3.finalEvent();
        refC1.t  = sync; refC1.finalEvent();
        refC2.t  = sync; refC2.finalEvent();
        refC3.t  = sync; refC3.finalEvent();


        System.out.println("- Write AView ------------------------");
        ObjectNode aTestView = async.readView(tenantIdAndCentricId, actorId, new ObjectId("ATest", a1.id.getId()));
        System.out.println(mapper.writeValueAsString(aTestView));
        ObjectNode aTestView1 = sync.readView(tenantIdAndCentricId, actorId, new ObjectId("ATest", a1.id.getId()));
        System.out.println("- vs - ");
        System.out.println(mapper.writeValueAsString(aTestView1));
        Assert.assertEquals(aTestView, aTestView1);



        System.out.println("- Write AViewDup-");
        ObjectNode aTestDupView = async.readView(tenantIdAndCentricId, actorId, new ObjectId("ATestDup", a1.id.getId()));
        System.out.println(mapper.writeValueAsString(aTestDupView));
        ObjectNode aTestDupView1 = sync.readView(tenantIdAndCentricId, actorId, new ObjectId("ATestDup", a1.id.getId()));
        System.out.println("- vs Read - ");
        System.out.println(mapper.writeValueAsString(aTestDupView1));
        Assert.assertEquals(aTestDupView, aTestDupView1);

        Assert.assertNotNull(aTestView);
        Assert.assertNotNull(aTestDupView);


        System.out.println("- Write CView -");
        ObjectNode cTestView = async.readView(tenantIdAndCentricId, actorId, new ObjectId("CTest", c1.id.getId()));
        System.out.println(mapper.writeValueAsString(cTestView));
        ObjectNode cTestView1 = sync.readView(tenantIdAndCentricId, actorId, new ObjectId("CTest", c1.id.getId()));
        System.out.println("- vs Read - ");
        System.out.println(mapper.writeValueAsString(cTestView1));
        Assert.assertEquals(cTestView, cTestView1);

        System.out.println("- Write CViewDup -");
        ObjectNode cTestDupView = async.readView(tenantIdAndCentricId, actorId, new ObjectId("CTestDup", c1.id.getId()));
        System.out.println(mapper.writeValueAsString(cTestDupView));
        ObjectNode cTestDupView1 = sync.readView(tenantIdAndCentricId, actorId, new ObjectId("CTestDup", c1.id.getId()));
        System.out.println("- vs Read - ");
        System.out.println(mapper.writeValueAsString(cTestDupView1));
        Assert.assertEquals(cTestDupView, cTestDupView1);

        Assert.assertNotNull(cTestView);
        Assert.assertNotNull(cTestDupView);

    }


    void run() {

    }

    class FireableValue implements Fireable<String> {
        TasmoMaterializerHarness t;
        private final long instanceId;
        private final String eventClassName;
        private final String[] fieldNames;
        String[] fieldValues;
        String[] finalFieldValues;
        int fieldValue;
        ObjectId id;
        Event lastEvent;

        public FireableValue(TasmoMaterializerHarness t, long instanceId, String eventClassName, String[] fieldNames, String[] finalFieldValues) {
            this.t =t;
            this.instanceId = instanceId;
            this.eventClassName = eventClassName;
            this.fieldNames = fieldNames;
            this.fieldValues = new String[fieldNames.length];
            this.finalFieldValues = finalFieldValues;
        }

        public ObjectId id() {
            return new ObjectId(eventClassName, new Id(instanceId));
        }

        @Override
        public void finalEvent() throws Exception {
            if (finalFieldValues == null) {
                remove();
            } else {
                EventBuilder create = EventBuilder.create(new ConstantIdProvider(instanceId), eventClassName, tenantId, actorId);
                int i = 0;
                for (String fieldName : fieldNames) {
                    String value = finalFieldValues[i];
                    fieldValues[i] = value;
                    i++;
                    fieldValue++;
                    if (value == null) {
                        create.clear(fieldName);
                    } else {
                        create.set(fieldName, value);
                    }

                }
                lastEvent = create.build();
                id = t.write(create.build());
            }
        }

        @Override
        public void create() throws EventWriteException {
            EventBuilder create = EventBuilder.create(new ConstantIdProvider(instanceId), eventClassName, tenantId, actorId);
            int i = 0;
            for (String fieldName : fieldNames) {
                String value = "value" + fieldValue;
                fieldValues[i] = value;
                i++;
                fieldValue++;
                create.set(fieldName, value);

            }
            lastEvent = create.build();
            id = t.write(create.build());
            //System.out.println("CREATE NODE " + id);
        }

        @Override
        public void update() throws EventWriteException {
            if (id != null) {
                EventBuilder update = EventBuilder.update(id, tenantId, actorId);
                int i = 0;
                for (String fieldName : fieldNames) {
                    String value = "value" + fieldValue;
                    fieldValues[i] = value;
                    i++;
                    fieldValue++;
                    update.set(fieldName, value);
                }
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("UPDATE NODE " + id);
            }
        }

        @Override
        public void remove() throws EventWriteException {
            if (id != null) {
                EventBuilder update = EventBuilder.update(id, tenantId, actorId);
                update.set(ReservedFields.DELETED, true);
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("REMOVE NODE " + id);
                id = null;
            }
        }

        @Override
        public ObjectId exists() {
            return id;
        }

        @Override
        public String toString() {
            return "FireableValue{"
                + "instanceId=" + instanceId
                + ", eventClassName=" + eventClassName
                + ", fieldNames=" + Arrays.toString(fieldNames)
                + ", fieldValues=" + Arrays.toString(fieldValues)
                + ", fieldValue=" + fieldValue
                + ", id=" + id
                + '}';
        }

    }

    class FireableRef implements Fireable<FireableValue> {
        TasmoMaterializerHarness t;
        private final FireableValue id;
        private final String fieldName;
        private final List<FireableValue> possibleRefs;
        private final FireableValue finalValue;
        private FireableValue fieldValue;
        private ObjectId lastEdge;
        private Event lastEvent;

        public FireableRef(TasmoMaterializerHarness t, FireableValue id, String fieldName, List<FireableValue> possibleRefs, FireableValue finalValue) {
            this.t = t;
            this.id = id;
            this.fieldName = fieldName;
            this.possibleRefs = possibleRefs;
            this.finalValue = finalValue;
        }

        @Override
        public void finalEvent() throws Exception {
            ObjectId fromInstanceId = id.id();
            fieldValue = finalValue;
            if (fieldValue == null) {
                remove();
            } else {
                ObjectId toInstanceId = finalValue.id();
                lastEdge = toInstanceId;
                EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                update.set("ref_" + fieldName, toInstanceId);
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("FINAl EDGE:" + fromInstanceId + " to " + toInstanceId);
            }
        }

        @Override
        public void create() throws EventWriteException {
            ObjectId fromInstanceId = id.exists();
            if (fromInstanceId != null) {
                fieldValue = possibleRefs.get(rand.nextInt(possibleRefs.size()));
                if (fieldValue != null) {
                    ObjectId toInstanceId = fieldValue.exists();
                    if (toInstanceId != null) {
                        lastEdge = toInstanceId;
                        EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                        update.set("ref_" + fieldName, toInstanceId);
                        lastEvent = update.build();
                        t.write(lastEvent);
                        //System.out.println("CREATED EDGE:" + fromInstanceId + " to " + toInstanceId);
                    }
                }
            }
        }

        @Override
        public void update() throws EventWriteException {
            ObjectId fromInstanceId = id.exists();
            if (fromInstanceId != null) {
                fieldValue = possibleRefs.get(rand.nextInt(possibleRefs.size()));
                if (fieldValue != null) {
                    ObjectId toInstanceId = fieldValue.exists();
                    if (toInstanceId != null) {
                        lastEdge = fromInstanceId;
                        EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                        update.set("ref_" + fieldName, toInstanceId);
                        t.write(lastEvent);
                        //System.out.println("UPDATED EDGE:" + fromInstanceId + " to " + toInstanceId);
                    }
                }
            }
        }

        @Override
        public void remove() throws EventWriteException {
            if (lastEdge != null) {
                fieldValue = null;
                EventBuilder update = EventBuilder.update(lastEdge, tenantId, actorId);
                update.clear("ref_" + fieldName);
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("REMOVED EDGE:" + lastEdge);
                lastEdge = null;
            }
        }

        @Override
        public ObjectId exists() {
            return lastEdge;
        }

        @Override
        public String toString() {
            return "FireableRef{"
                + "id=" + id
                + ", fieldName=" + fieldName
                + ", possibleRefs=" + possibleRefs
                + ", fieldValue=" + fieldValue
                + ", lastEdge=" + lastEdge
                + '}';
        }

    }

    class FireableRefs implements Fireable<FireableValue> {

        private final TasmoMaterializerHarness t;
        private final FireableValue id;
        private final String fieldName;
        private final List<FireableValue> possibleRefs;
        private List<FireableValue> fieldValues;
        private ObjectId lastEdge;
        private Event lastEvent;

        public FireableRefs(TasmoMaterializerHarness t, FireableValue id, String fieldName, List<FireableValue> possibleRefs) {
            this.t = t;
            this.id = id;
            this.fieldName = fieldName;
            this.possibleRefs = possibleRefs;
        }

        @Override
        public void finalEvent() throws Exception {
        }

        List<FireableValue> pickFromPossible() {
            List<Integer> is = new ArrayList<>();
            for (int i = 0; i < possibleRefs.size(); i++) {
                is.add(i);
            }
            Collections.shuffle(is, rand);
            List<FireableValue> picked = new ArrayList<>();
            int pick = 1 + rand.nextInt(possibleRefs.size() - 1);
            for (int i = 0; i < pick; i++) {
                picked.add(possibleRefs.get(is.get(i)));
            }
            return picked;
        }

        List<ObjectId> fieldValueToInstanceId(List<FireableValue> fireableValues) {
            List<ObjectId> ids = new ArrayList<>();
            for (FireableValue fireableValue : fireableValues) {
                ObjectId id = fireableValue.exists();
                if (id != null) {
                    ids.add(id);
                }
            }
            return ids;
        }

        @Override
        public void create() throws EventWriteException {
            ObjectId fromInstanceId = id.exists();
            if (fromInstanceId != null) {
                lastEdge = fromInstanceId;
                fieldValues = pickFromPossible();
                EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                update.set("refs_" + fieldName, fieldValueToInstanceId(fieldValues));
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("CREATED EDGE:" + fromInstanceId + " to " + toInstanceId);
            }
        }

        @Override
        public void update() throws EventWriteException {
            ObjectId fromInstanceId = id.exists();
            if (fromInstanceId != null) {
                lastEdge = fromInstanceId;
                fieldValues = pickFromPossible();
                EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                update.set("refs_" + fieldName, fieldValueToInstanceId(fieldValues));
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("UPDATE EDGE:" + fromInstanceId + " to " + toInstanceId);
            }
        }

        @Override
        public void remove() throws EventWriteException {
            if (lastEdge != null) {
                fieldValues = null;
                EventBuilder update = EventBuilder.update(lastEdge, tenantId, actorId);
                update.clear("refs_" + fieldName);
                lastEvent = update.build();
                t.write(lastEvent);
                //System.out.println("REMOVED EDGE:" + lastEdge);
                lastEdge = null;
            }
        }

        @Override
        public ObjectId exists() {
            return lastEdge;
        }

        @Override
        public String toString() {
            return "FireableRef{"
                + "id=" + id
                + ", fieldName=" + fieldName
                + ", possibleRefs=" + possibleRefs
                + ", fieldValues=" + fieldValues
                + ", lastEdge=" + lastEdge
                + '}';
        }

    }

    interface Fireable<V> {

        void create() throws Exception;

        void update() throws Exception;

        void remove() throws Exception;

        ObjectId exists();

        void finalEvent() throws Exception;

    }

    class RandomFireable<V> implements Callable<Void> {

        private final Fireable<V> fireable;
        private final long delay;
        private final long duration;
        private final CountDownLatch latch;
        long lifespan;

        public RandomFireable(Fireable<V> fireable,
            long delay,
            long duration,
            CountDownLatch latch) {
            this.fireable = fireable;
            this.delay = delay;
            this.duration = duration;
            this.latch = latch;
        }

        @Override
        public Void call() throws Exception {
            try {
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - duration < start) {
                    if (fireable.exists() != null) {
                        if (rand.nextBoolean()) {
                            fireable.update();
                        } else {
                            fireable.remove();
                        }
                    } else {
                        fireable.create();
                    }
                    Thread.sleep(delay);
                }

                fireable.finalEvent();
                lifespan = System.currentTimeMillis() - start;
                System.out.println("Done " + fireable);
                return null;
            } finally {
                latch.countDown();
            }
        }

    }

    class ConstantIdProvider implements IdProvider {

        private final Id id;

        public ConstantIdProvider(long id) {
            this.id = new Id(id);
        }

        @Override
        public Id nextId() {
            return id;
        }

    }

}

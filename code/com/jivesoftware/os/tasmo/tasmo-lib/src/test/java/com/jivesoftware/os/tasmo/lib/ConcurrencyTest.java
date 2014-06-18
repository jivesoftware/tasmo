package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProvider;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class ConcurrencyTest extends BaseTasmoTest {

    public static final Random rand = new Random();

    @Test (enabled = false, invocationCount = 10, singleThreaded = true)
    public void concurrencyTest() throws Exception {

        // Folder->Doc->User
        String[] binding = new String[]{
            //"ATest::1::A.x"
            //,"ATest::2::A.ref_toB.ref.B|B.i"
            //,"ATest::3::A.ref_toB.ref.B|B.j"
            "ATest::4::A.ref_toB.ref.B|B.ref_toC.ref.C|C.a,b,c", "CTest::5::C.backRefs.B.ref_toC|B.backRefs.A.ref_toB|A.x"
        };

        Expectations expectations = initModelPaths(binding);

        FireableValue a1 = new FireableValue(1_000_000, "A", new String[]{ "x", "y", "z" });
        FireableValue b1 = new FireableValue(1_000_002, "B", new String[]{ "h", "i", "j" });
        FireableValue c1 = new FireableValue(1_000_004, "C", new String[]{ "a", "b", "c" });

        FireableRef refA = new FireableRef(a1, "toB", Arrays.asList(b1));
        FireableRef refB = new FireableRef(b1, "toC", Arrays.asList(c1));

        //ExecutorService threads = MoreExecutors.sameThreadExecutor();
        ExecutorService threads = Executors.newCachedThreadPool();
        List<RandomFireable<?>> fireables = new ArrayList<>();

        fireables.add(new RandomFireable<>(a1, true, 1, 250));
        fireables.add(new RandomFireable<>(b1, true, 1, 250));
        fireables.add(new RandomFireable<>(c1, true, 1, 250));
        fireables.add(new RandomFireable<>(refA, true, 10, 2000));
        fireables.add(new RandomFireable<>(refB, true, 10, 2000));
        threads.invokeAll(fireables);

        threads.shutdown();
        threads.awaitTermination(30, TimeUnit.SECONDS);


        for(RandomFireable rf:fireables) {
            System.out.println(rf.lifespan);
        }

//        a1.create();
//        b1.create();
//        c1.create();
//        refA.create();
//        refB.create();


        System.out.println(a1.id);
        System.out.println(b1.id);
        System.out.println(c1.id);
        System.out.println(refA.lastEdge);
        System.out.println(refB.lastEdge);


        System.out.println("-------------------------");
        ObjectNode view = readView(tenantIdAndCentricId, actorId, new ObjectId("ATest", a1.id.getId()));
        System.out.println(mapper.writeValueAsString(view));
        System.out.println("-------------------------");
        view = readView(tenantIdAndCentricId, actorId, new ObjectId("CTest", c1.id.getId()));
        System.out.println(mapper.writeValueAsString(view));
        Assert.assertNotNull(view);

    }
    class FireableValue implements Fireable<String> {

        private final long instanceId;
        private final String eventClassName;
        private final String[] fieldNames;
        String[] fieldValues;
        int fieldValue;
        ObjectId id;

        public FireableValue(long instanceId, String eventClassName, String[] fieldNames) {
            this.instanceId = instanceId;
            this.eventClassName = eventClassName;
            this.fieldNames = fieldNames;
            this.fieldValues = new String[fieldNames.length];
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
            id = write(create.build());
            System.out.println("CREATE NODE "+id);
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
                write(update.build());
                System.out.println("UPDATE NODE "+id);
            }
        }

        @Override
        public void remove() throws EventWriteException {
            EventBuilder update = EventBuilder.update(id, tenantId, actorId);
            update.set(ReservedFields.DELETED, true);
            write(update.build());
            System.out.println("REMOVE NODE "+id);
            id = null;
        }

        @Override
        public ObjectId exists() {
            return id;
        }

    }

    class FireableRef implements Fireable<FireableValue> {

        private final FireableValue id;
        private final String fieldName;
        private final List<FireableValue> possibleRefs;
        private FireableValue fieldValue;
        private ObjectId lastEdge;

        public FireableRef(FireableValue id, String fieldName, List<FireableValue> possibleRefs) {
            this.id = id;
            this.fieldName = fieldName;
            this.possibleRefs = possibleRefs;
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
                        write(update.build());
                        System.out.println("CREATED EDGE:" + fromInstanceId + " to " + toInstanceId);
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
                        lastEdge = toInstanceId;
                        EventBuilder update = EventBuilder.update(fromInstanceId, tenantId, actorId);
                        update.set("ref_" + fieldName, toInstanceId);
                        write(update.build());
                        System.out.println("UPDATED EDGE:" + fromInstanceId + " to " + toInstanceId);
                    }
                }
            }
        }

        @Override
        public void remove() throws EventWriteException {
            if (fieldValue != null) {
                fieldValue = null;
                EventBuilder update = EventBuilder.update(id.id, tenantId, actorId);
                update.clear("ref_" + fieldName);
                write(update.build());
                System.out.println("REMOVED EDGE:" + lastEdge);
                lastEdge = null;
            }
        }

        @Override
        public ObjectId exists() {
            return id.id;
        }

    }

    interface Fireable<V> {

        void create() throws Exception;

        void update() throws Exception;

        void remove() throws Exception;

        ObjectId exists();

    }

    class RandomFireable<V> implements Callable<Void> {

        private final Fireable<V> fireable;
        private final boolean mustExistWhenDone;
        private final long delay;
        private final long duration;
        long lifespan;

        public RandomFireable(Fireable<V> fireable,
            boolean existsWhenDone,
            long delay,
            long duration) {
            this.fireable = fireable;
            this.mustExistWhenDone = existsWhenDone;
            this.delay = delay;
            this.duration = duration;
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

                if (mustExistWhenDone) {
                    if (fireable.exists() == null) {
                        fireable.create();
                    }
                } else {
                    if (fireable.exists() != null) {
                        fireable.remove();
                    }
                }
                lifespan = System.currentTimeMillis()-start;
            } catch (Exception x) {
                x.printStackTrace();
            }
            return null;
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


    class AssertExpectation {

        Expectations expectations;
        String viewClassName;
        String viewFieldName;
        FireableRef[] fireables;

        public AssertExpectation(Expectations expectations, String viewClassName, String viewFieldName, FireableRef[] fireables) {
            this.expectations = expectations;
            this.viewClassName = viewClassName;
            this.viewFieldName = viewFieldName;
            this.fireables = fireables;
        }

        //expectations.addExpectation(content1, viewClassName, viewFieldName, new ObjectId[]{ content1, user1 }, "userName", "ted");
        void assertExpectation() {
            expectations.clear();
            List<ObjectId> pathIds = new ArrayList<>();
            ObjectId rootId = fireables[0].exists();
            if (rootId == null) {
                return; // Doesn't exists so nothing to assert
            }
            pathIds.add(rootId);

            for (int i = 0; i < fireables.length; i++) {
                ObjectId toId = fireables[i].exists();

            }
            expectations.addExpectation(pathIds.get(0), viewClassName, viewFieldName, pathIds.toArray(new ObjectId[pathIds.size()]), viewFieldName, views);
        }
    }
}

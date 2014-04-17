/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantIdAndRow;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 *
 */
public class CombinatorialMaterializerTest {

    //private long seed = System.currentTimeMillis();
    private final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final long seed = 1389045159990L;
    private final boolean verbose = false;
    private final int maxStepDepth = 4; // TODO change back to 4
    private final int maxFanOut = 2;
    //private final List<ModelPathStepType> stepTypes = new ArrayList<>(Arrays.asList(ModelPathStepType.backRefs, ModelPathStepType.value));
    private final List<ModelPathStepType> stepTypes = new ArrayList<>(Arrays.asList(ModelPathStepType.values()));
    private final Executor executor = Executors.newCachedThreadPool();

    private void println(Object line) {
        if (verbose) {
            LOG.info(line == null ? "null" : line.toString());
        }
    }

    @BeforeClass
    public void logger() {
        String PATTERN = "%t %m%n";

        Enumeration allAppenders = LogManager.getRootLogger().getAllAppenders();
        while (allAppenders.hasMoreElements()) {
            Appender appender = (Appender) allAppenders.nextElement();
            appender.setLayout(new PatternLayout(PATTERN));
        }
        if (verbose) {
            LogManager.getLogger("com.jivesoftware.os.tasmo").setLevel(Level.TRACE);
            LogManager.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange").setLevel(Level.TRACE);
            LogManager.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore").setLevel(Level.TRACE);
            LogManager.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore").setLevel(Level.TRACE);
        } else {
            LogManager.getRootLogger().setLevel(Level.OFF);
        }
    }

    @Test(dataProvider = "totalOrderAdds", invocationCount = 1, singleThreaded = true)
    public void testSingleThreadedTotalOrderAdds(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, false);
    }

    @Test(dataProvider = "unorderedAdds", invocationCount = 1, singleThreaded = true)
    public void testSingleThreadedUnorderedAdds(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, false);
    }

    @Test(dataProvider = "totalOrderAdds", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsOnly(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, true);
    }

    @Test(dataProvider = "addsThenRemoves", invocationCount = 1, singleThreaded = true)
    public void testSingleThreadedAddsThenRemoves(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, false);
    }

    @Test(dataProvider = "addsThenRemoves", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsThenRemoves(InputCase inputCase)
            throws Throwable {

        assertCombination(inputCase, null, true);
    }

    @Test(dataProvider = "addsThenRemovesThenAdds", invocationCount = 1, singleThreaded = true)
    public void testSingleThreadedAddsThenRemovesThenAdds(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, false);
    }

    @Test(dataProvider = "addsThenRemovesThenAdds", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsThenRemovesThenAdds(InputCase inputCase)
            throws Throwable {
        assertCombination(inputCase, null, true);
    }

    private void assertCombination(final InputCase ic, Long onlyRunTestId, boolean multiThreadWrites) throws Throwable {
        try {
            if (ic.testId % 1000 == 0) {
                if (ic.testId == 0) {
                    System.out.println("***** Begin test category:" + ic.category + " multi-threaded:" + multiThreadWrites + " ********");
                } else {
                    System.out.println("***** Ran " + ic.testId + " for category:" + ic.category + " multi-threaded:" + multiThreadWrites + " ********");
                }
            }
            if (onlyRunTestId != null && onlyRunTestId != ic.testId) {
                return;
            }
            println("***** category:" + ic.category + " testId:" + ic.testId + " BINDING *****");
            println(ic.binding);
            Expectations expectations = ic.materialization.initModelPaths(tenantIdAndCentricId.getTenantId(), Arrays.asList(ic.binding));

            List<Event> firedEvents = ic.input.getFiredEvents();
            println("***** category:" + ic.category + " testId:" + ic.testId + " EVENTS (" + firedEvents.size() + ") *****");
            for (Event evt : firedEvents) {
                println(evt);
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " FIRING EVENTS (" + firedEvents.size() + ") *****");
            if (multiThreadWrites) {
                fireEventInParallel(firedEvents, ic);
            } else {
                for (Event evt : firedEvents) {
                    ic.eventWriterProvider.eventWriter().write(evt);
                }
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " BUILDING ASSERTIONS *****");
            expectations.buildExpectations(ic.testId, expectations, ic.binding, ic.input, ic.deletedId);

            println("***** category:" + ic.category + " testId:" + ic.testId + " ASSERTING *****");
            expectations.assertExpectation(tenantIdAndCentricId);
            expectations.clear();

            println("***** category:" + ic.category + " testId:" + ic.testId + " OUTPUT *****");
            ObjectNode view = ic.materialization.readView(tenantIdAndCentricId, actorId, ic.input.getViewId());
            println(ic.materialization.mapper.writeValueAsString(view));

            if (!ic.category.equals("removes")) {
                List<AssertionResult> allBranchResults = new ArrayList<>();
                assertViewElementExists(ic.binding.getModelPaths().get(0).getPathMembers(), 0, view, ic.input.getLeafNodeFields(), allBranchResults);

                for (AssertionResult result : allBranchResults) {
                    Assert.assertTrue(result.isPassed(), result.getMessage());
                }
            }

            println("***** category:" + ic.category + " testId:" + ic.testId + " PASSED *****");

        } catch (Throwable t) {
            System.out.println("Test:testAllModelPathCombinationsAndEventFireCombinations: category:" + ic.category
                    + " testId:" + ic.testId + " seed:" + seed + " Failed.");
            t.printStackTrace();

            if (verbose) {
                System.out.println(Thread.currentThread() + " |--> testId:" + ic.testId + " seed:" + seed + " Failed.");
                System.out.println(Thread.currentThread() + " |--> REASON ");
                System.out.println(Thread.currentThread() + " |--> " + t.getMessage() + " " + t.getClass());
                System.out.println(Thread.currentThread() + " |--> FAILED " + verbose);

                ic.materialization.rawViewValueStore.getAllRowKeys(100, null, new CallbackStream<TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray>>() {

                    @Override
                    public TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray> callback(
                            final TenantIdAndRow<TenantIdAndCentricId, ImmutableByteArray> row) throws Exception {
                        if (row != null) {
                            ic.materialization.rawViewValueStore.getEntrys(row.getTenantId(), row.getRow(), null, Long.MAX_VALUE, 1000, false, null, null,
                                    new CallbackStream<ColumnValueAndTimestamp<ImmutableByteArray, String, Long>>() {

                                        @Override
                                        public ColumnValueAndTimestamp<ImmutableByteArray, String, Long> callback(
                                                ColumnValueAndTimestamp<ImmutableByteArray, String, Long> value) throws Exception {
                                                    if (value != null) {

                                                        System.out.println(" |--> " + rowKey(row.getRow())
                                                                + " | " + columnKey(value.getColumn())
                                                                + " | " + value.getValue() + " | " + value.getTimestamp());
                                                    }
                                                    return value;
                                                }
                                    });
                        }
                        return row;
                    }
                });
                System.exit(0);
            }
            throw t;
        }
    }

    private void fireEventInParallel(List<Event> batch, final InputCase ic) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(batch.size());
        final AtomicLong errors = new AtomicLong();
        final JsonEventConventions jec = new JsonEventConventions();
        for (final Event b : batch) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    Thread.currentThread().setName("thread-for-event-" + jec.getEventId(b.toJson()));
                    try {
                        ic.eventWriterProvider.eventWriter().write(Arrays.asList(b));
                    } catch (EventWriteException ex) {
                        ex.printStackTrace();
                        errors.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        if (errors.get() > 0) {
            if (verbose) {
                println("Encountered errors while sending events.");
                System.exit(0);
            }
            Assert.fail("Encountered errors while sending events.");
        }
    }

    public ObjectId rowKey(ImmutableByteArray viewObjectId) throws IOException {
        return new ObjectId(ByteBuffer.wrap(viewObjectId.getImmutableBytes()));
    }

    private String columnKey(ImmutableByteArray bytes) throws IOException {
        StringBuilder sb = new StringBuilder();
        ByteBuffer buf = ByteBuffer.wrap(bytes.getImmutableBytes());
        int pathHashCode = buf.getInt();
        int classesHashCode = buf.getInt();
        sb.append(pathHashCode).append(".").append(classesHashCode).append("[");
        while (buf.remaining() > 0) {
            byte length = buf.get();
            byte[] id = new byte[length];
            buf.get(id);
            sb.append(new Id(id).toStringForm()).append(" ");
        }
        sb.append(']');
        return sb.toString();
    }

    private final OrderIdProviderGenerator orderIdProviderGenerator = new OrderIdProviderGenerator();
    private final TenantId tenantId = new TenantId("test");
    private final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
    private final Id actorId = new Id(100000000);
    private final ModelPathGenerator pathGenerator = new ModelPathGenerator();
    private final EventFireGenerator eventFireGenerator = new EventFireGenerator(tenantId, actorId);

    @DataProvider(name = "totalOrderAdds")
    public Iterator<Object[]> provideTotalOrderAdds() throws Exception {
        List<ViewBinding> viewBindings = buildBindings(stepTypes, maxStepDepth);

        final ViewBinding binding = viewBindings.get(0);

        // Straight Adds
        List<ModelPath> modelPaths = viewBindings.get(0).getModelPaths();
        return Iterators.transform(modelPaths.iterator(), new Function<ModelPath, Object[]>() {
            long testId = 0;

            @Override
            public Object[] apply(ModelPath path) {
                OrderIdProvider idProvider = monatomic(0);
                IdProviderImpl idProviderImpl = new IdProviderImpl(idProvider);
                EventsAndViewId deriedEventsAndViewId = eventFireGenerator.deriveEventsFromPath(idProviderImpl, path, idProviderImpl.nextId(), maxFanOut);
                ObjectId viewId = new ObjectId(binding.getViewClassName(), deriedEventsAndViewId.getViewId());
                long highestId = idProvider.nextId();

                Materialization materialization = new Materialization();
                try {
                    materialization.setupModelAndMaterializer();
                } catch (Exception x) {
                    throw new RuntimeException("Failed to setupModelAndMaterializer()" + x);
                }
                idProvider = monatomic(highestId);
                EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);
                Set<Id> deletedIds = new HashSet<>();

                // Case: Entirely new adds
                EventFire eventFire = new EventFire(viewId,
                        deriedEventsAndViewId.getEvents(),
                        path.getPathMembers().get(path.getPathMemberSize() - 1),
                        deriedEventsAndViewId.getIdTree());

                Object[] buildParamaterListItem = buildParamaterListItem("totalOrderAdds",
                        testId, materialization, tenantIdAndCentricId, actorId, binding, path, writerProvider, eventFire, deletedIds);
                testId++;
                return buildParamaterListItem;
            }
        });

    }

    @DataProvider(name = "unorderedAdds")
    public Iterator<Object[]> provideUnorderedAdds() throws Exception {
        List<ViewBinding> viewBindings = buildBindings(stepTypes, maxStepDepth);

        final ViewBinding binding = viewBindings.get(0);
        final int randomBatchSize = 2;

        List<ModelPath> modelPaths = viewBindings.get(0).getModelPaths();
        // Shuffled Adds
        Iterator<Iterator<Object[]>> transform = Iterators.transform(modelPaths.iterator(), new Function<ModelPath, Iterator<Object[]>>() {
            long testId = 0;

            @Override
            public Iterator<Object[]> apply(final ModelPath path) {
                OrderIdProvider initialIdProvider = monatomic(0);
                IdProviderImpl idProviderImpl = new IdProviderImpl(initialIdProvider);
                final EventsAndViewId deriedEventsAndViewId = eventFireGenerator.deriveEventsFromPath(idProviderImpl, path, idProviderImpl.nextId(), maxFanOut);
                final ObjectId viewId = new ObjectId(binding.getViewClassName(), deriedEventsAndViewId.getViewId());
                final Set<Id> deletedIds = new HashSet<>();
                long highestId = initialIdProvider.nextId();

                // Case: Entirely new adds with shuffled eventIds. Ensures delivery and time stamp order doesn't matter.
                List<OrderIdProvider> idProviders = orderIdProviderGenerator.generateOrderIdProviders(seed, highestId,
                        new IdBatchConfig(Order.shuffle, deriedEventsAndViewId.getEvents().size(), randomBatchSize));

                return Iterators.transform(idProviders.iterator(), new Function<OrderIdProvider, Object[]>() {

                    @Override
                    public Object[] apply(OrderIdProvider idProvider) {
                        Materialization materialization = new Materialization();
                        try {
                            materialization.setupModelAndMaterializer();
                        } catch (Exception x) {
                            throw new RuntimeException("Failed to setupModelAndMaterializer()" + x);
                        }
                        EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);
                        EventFire eventFire = new EventFire(viewId,
                                deriedEventsAndViewId.getEvents(),
                                path.getPathMembers().get(path.getPathMemberSize() - 1),
                                deriedEventsAndViewId.getIdTree());

                        Object[] buildParamaterListItem = buildParamaterListItem("unorderedAdds",
                                testId, materialization, tenantIdAndCentricId, actorId, binding, path, writerProvider, eventFire, deletedIds);
                        testId++;
                        return buildParamaterListItem;
                    }

                });
            }

        });
        return Iterators.concat(transform);
    }

    @DataProvider(name = "addsThenRemoves")
    public Iterator<Object[]> provideAddsThenRemoves() throws Exception {
        List<ViewBinding> viewBindings = buildBindings(stepTypes, maxStepDepth);

        List<Object[]> paramList = new ArrayList<>();
        ViewBinding binding = viewBindings.get(0);
        long testId = 0;
        int randomBatchSize = 2;

        for (ModelPath path : viewBindings.get(0).getModelPaths()) {

            OrderIdProvider initialIdProvider = monatomic(0);
            IdProviderImpl idProviderImpl = new IdProviderImpl(initialIdProvider);
            EventsAndViewId deriedEventsAndViewId = eventFireGenerator.deriveEventsFromPath(idProviderImpl, path, idProviderImpl.nextId(), maxFanOut);
            ObjectId viewId = new ObjectId(binding.getViewClassName(), deriedEventsAndViewId.getViewId());
            long highestId = initialIdProvider.nextId();

            // Case: Entirely new adds with shuffled eventIds. Ensures delivery and time stamp order doesn't matter.
            EventFire eventFire = new EventFire(viewId,
                    deriedEventsAndViewId.getEvents(),
                    path.getPathMembers().get(path.getPathMemberSize() - 1),
                    deriedEventsAndViewId.getIdTree());

            for (int i = 0; i < path.getPathMemberSize(); i++) {

                for (int j = 1; j < 2; j++) {

                    // Case: build up and then delete at a depth and breadth delete
                    List<Event> deleteEvents = eventFire.createDeletesAtDepth(tenantId, actorId, i, j);
                    Set<Id> deletedIds = new HashSet<>();
                    for (Event evt : deleteEvents) {
                        deletedIds.add(evt.getObjectId().getId());
                    }
                    List<Event> events = deriedEventsAndViewId.getEvents();

                    for (OrderIdProvider idProvider : orderIdProviderGenerator.generateOrderIdProviders(seed, highestId,
                            new IdBatchConfig(Order.shuffle, events.size(), randomBatchSize),
                            new IdBatchConfig(Order.shuffle, deleteEvents.size(), randomBatchSize))) {

                        Materialization materialization = new Materialization();
                        materialization.setupModelAndMaterializer();
                        EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);

                        List<Event> allEvents = new ArrayList<>();
                        allEvents.addAll(events);
                        allEvents.addAll(deleteEvents);

                        // We have to allocate eventIds up front for determinism
                        JsonEventConventions jec = new JsonEventConventions();
                        for (Event e : allEvents) {
                            jec.setEventId(e.toJson(), idProvider.nextId());
                        }

                        eventFire = new EventFire(viewId,
                                allEvents,
                                path.getPathMembers().get(path.getPathMemberSize() - 1),
                                deriedEventsAndViewId.getIdTree());

                        paramList.add(buildParamaterListItem("addsThenRemoves",
                                testId, materialization, tenantIdAndCentricId, actorId, binding, path, writerProvider, eventFire, deletedIds));
                        testId++;
                    }
                }
            }
        }

        return paramList.iterator();
    }

    @DataProvider(name = "addsThenRemovesThenAdds")
    public Iterator<Object[]> provideAddsThenRemovesThenAdds() throws Exception {
        List<ViewBinding> viewBindings = buildBindings(stepTypes, maxStepDepth);

        List<Object[]> paramList = new ArrayList<>();
        ViewBinding binding = viewBindings.get(0);
        long testId = 0;
        int randomBatchSize = 2;

        for (ModelPath path : viewBindings.get(0).getModelPaths()) {

            OrderIdProvider initialIdProvider = monatomic(0);
            IdProviderImpl idProviderImpl = new IdProviderImpl(initialIdProvider);
            EventsAndViewId deriedEventsAndViewId = eventFireGenerator.deriveEventsFromPath(idProviderImpl, path, idProviderImpl.nextId(), maxFanOut);
            ObjectId viewId = new ObjectId(binding.getViewClassName(), deriedEventsAndViewId.getViewId());
            long highestId = initialIdProvider.nextId();

            // Case: Entirely new adds with shuffled eventIds. Ensures delivery and time stamp order doesn't matter.
            EventFire eventFire = new EventFire(viewId,
                    deriedEventsAndViewId.getEvents(),
                    path.getPathMembers().get(path.getPathMemberSize() - 1),
                    deriedEventsAndViewId.getIdTree());

            JsonEventConventions jec = new JsonEventConventions();
            for (int i = 0; i < path.getPathMemberSize(); i++) {

                for (int j = 1; j < 3; j++) {

                    // Case: build up and then delete at a depth and breadth delete
                    List<Event> deleteEvents = eventFire.createDeletesAtDepth(tenantId, actorId, i, j);
                    Set<Id> deletedIds = new HashSet<>();
                    for (Event evt : deleteEvents) {
                        deletedIds.add(evt.getObjectId().getId());
                    }
//                    if (deletedIds.size() > 1) {
//                        System.out.println("JBOOYA "+deletedIds.size());
//                    }
                    List<Event> events = deriedEventsAndViewId.getEvents();

                    List<Event> undeletes = new ArrayList<>();
                    for (Event event : events) {
                        ObjectId instanceObjectId = jec.getInstanceObjectId(event.toJson());
                        if (deletedIds.contains(instanceObjectId.getId())) {
                            undeletes.add(new Event(event.toJson().deepCopy(), event.getObjectId()));
                        }
                    }

                    for (OrderIdProvider idProvider : orderIdProviderGenerator.generateOrderIdProviders(seed, highestId,
                            new IdBatchConfig(Order.shuffle, events.size(), randomBatchSize),
                            new IdBatchConfig(Order.shuffle, deleteEvents.size(), randomBatchSize),
                            new IdBatchConfig(Order.shuffle, undeletes.size(), randomBatchSize))) {

                        Materialization materialization = new Materialization();
                        materialization.setupModelAndMaterializer();
                        EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);

                        List<Event> allEvents = new ArrayList<>();
                        allEvents.addAll(events);
                        allEvents.addAll(deleteEvents);
                        allEvents.addAll(undeletes);

                        // We have to allocate eventIds up front for determinism
                        jec = new JsonEventConventions();
                        for (Event e : allEvents) {
                            jec.setEventId(e.toJson(), idProvider.nextId());
                        }

                        eventFire = new EventFire(viewId,
                                allEvents,
                                path.getPathMembers().get(path.getPathMemberSize() - 1),
                                deriedEventsAndViewId.getIdTree());

                        paramList.add(buildParamaterListItem("addsThenRemovesThenAdds",
                                testId, materialization, tenantIdAndCentricId, actorId, binding, path, writerProvider, eventFire, new HashSet<Id>()));
                        testId++;
                    }
                }
            }
        }

        return paramList.iterator();
    }

    public List<ViewBinding> buildBindings(List<ModelPathStepType> refTypes, int maxNumSteps) throws Exception {
        Materialization materialization = new Materialization();
        materialization.setupModelAndMaterializer();

        List<String> pathStrings = pathGenerator.generateModelPaths(refTypes, maxNumSteps);
        List<ViewBinding> allViewBindings = materialization.parseModelPathStrings(pathStrings);

        if (allViewBindings.size() > 1) {
            throw new IllegalStateException("Unexpectedly generated model paths with more than one view class name");
        }
        return allViewBindings;
    }

    private EventWriterProvider buildEventWriterProvider(Materialization materialization, OrderIdProvider idProvider) {
        final EventWriter writer = new EventWriter(materialization.jsonEventWriter(materialization.materializer, idProvider));

        return new EventWriterProvider() {
            @Override
            public EventWriter eventWriter() {
                return writer;
            }
        };
    }

    private OrderIdProvider monatomic(final long initial) {
        return new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong(initial);

            @Override
            public long nextId() {
                return id.addAndGet(2); // Have to move by twos so there is room for add vs remove differentiation.
            }
        };
    }

    private Object[] buildParamaterListItem(String category, long testId, Materialization materialization,
            TenantIdAndCentricId tenantIdAndCentricId,
            Id actorId,
            ViewBinding binding,
            ModelPath path,
            EventWriterProvider writerProvider,
            EventFire eventFire,
            Set<Id> deletedIds) {
        return new Object[]{new InputCase(category,
            testId,
            materialization,
            tenantIdAndCentricId, actorId,
            new ViewBinding(binding.getViewClassName(), Arrays.asList(path), false, false, false, null),
            writerProvider,
            eventFire,
            deletedIds)};
    }

    static public class InputCase {

        public final String category;
        public final long testId;
        public final Materialization materialization;
        public final TenantIdAndCentricId tenantIdAndCentricId;
        public final Id actorId;
        public final ViewBinding binding;
        public final EventWriterProvider eventWriterProvider;
        public final EventFire input;
        public final Set<Id> deletedId;

        public InputCase(String category, long testId, Materialization materialization, TenantIdAndCentricId tenantIdAndCentricId, Id actorId,
                ViewBinding binding, EventWriterProvider eventWriterProvider, EventFire input,
                Set<Id> deletedId) {
            this.category = category;
            this.testId = testId;
            this.materialization = materialization;
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.actorId = actorId;
            this.binding = binding;
            this.eventWriterProvider = eventWriterProvider;
            this.input = input;
            this.deletedId = deletedId;
        }
    }

    //TODO use and communicate different leaf node fields per branch.
    public void assertViewElementExists(List<ModelPathStep> path, int pathIndex, ObjectNode viewNode,
            Map<String, String> expectedFieldValues, List<AssertionResult> resultAccumulator) {

        if (viewNode == null) {
            resultAccumulator.add(new AssertionResult(false, "Supplied view node is null"));
        } else if (pathIndex == path.size() - 1) {
            resultAccumulator.add(assertLeafNodeFields(viewNode, expectedFieldValues));
        } else {
            ModelPathStep step = path.get(pathIndex);
            String refField = step.getRefFieldName();
            if (refField == null) {
                throw new IllegalArgumentException("Malformed model path - ref field not present in mid-path element");
            }

            if (ModelPathStepType.backRefs.equals(step.getStepType())) {
                refField = "all_" + refField;
            } else if (ModelPathStepType.latest_backRef.equals(step.getStepType())) {
                refField = "latest_" + refField;
            } else if (ModelPathStepType.count.equals(step.getStepType())) {
                refField = "count_" + refField;
            }

            JsonNode nextNode = viewNode.get(refField);
            if (nextNode == null) {
                resultAccumulator.add(new AssertionResult(false, "No view data exists for path element " + step));
            } else if (nextNode.isArray()) { //handles refs and all_backrefs
                ArrayNode arrayNode = (ArrayNode) nextNode;
                if (arrayNode.size() == 0) {
                    resultAccumulator.add(new AssertionResult(false, "Empty array element in view data for path element " + step));
                } else {
                    for (Iterator<JsonNode> iter = arrayNode.elements(); iter.hasNext();) {
                        JsonNode element = iter.next();
                        if (element.isObject()) {
                            assertViewElementExists(path, pathIndex + 1, (ObjectNode) element, expectedFieldValues, resultAccumulator);
                        } else {
                            resultAccumulator.add(new AssertionResult(false, "Array element view data for path element " + step + " was not an object"));
                        }
                    }
                }
            } else if (nextNode.isObject()) { //handles ref and latest_backref
                assertViewElementExists(path, pathIndex + 1, (ObjectNode) nextNode, expectedFieldValues, resultAccumulator);
            } else if (nextNode.isInt()) { //handles count
                resultAccumulator.add(assertCountField((IntNode) nextNode, refField, maxFanOut));
            } else {
                resultAccumulator.add(new AssertionResult(false, "Element view data for path element " + step + " was an unexpected type: " + nextNode));
            }
        }
    }

    private AssertionResult assertCountField(IntNode leaf, String field, int expectedFieldValue) {
        if (leaf.intValue() == expectedFieldValue) {
            return new AssertionResult(true, "");
        } else {
            return new AssertionResult(false, "Unexpected value for field " + field + " - found " + leaf);
        }
    }

    private AssertionResult assertLeafNodeFields(ObjectNode leaf, Map<String, String> expectedFieldValues) {
        for (Map.Entry<String, String> entry : expectedFieldValues.entrySet()) {
            String field = entry.getKey();
            String expectedVal = entry.getValue();

            if (expectedVal == null) {
                throw new IllegalArgumentException("Expected field values cannot be null. Expectation for field " + field + " was null");
            }

            JsonNode value = leaf.get(field);
            if (value == null || value.isNull()) {
                return new AssertionResult(false, "Null value found for field " + field);
            }

            String toTest = value.asText();
            if (expectedVal.equals(toTest)) {
                return new AssertionResult(true, "");
            } else {
                return new AssertionResult(false, "Unexpected value for field " + field + " - found " + toTest);
            }
        }

        return new AssertionResult(true, "");

    }

    public static class AssertionResult {

        private final boolean passed;
        private final String message;

        public AssertionResult(boolean passed, String message) {
            this.passed = passed;
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public boolean isPassed() {
            return passed;
        }
    }
}

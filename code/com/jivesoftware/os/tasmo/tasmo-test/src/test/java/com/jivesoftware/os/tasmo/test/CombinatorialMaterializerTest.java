/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 *
 */
public class CombinatorialMaterializerTest {

    private final long seed = System.currentTimeMillis();
    private final boolean verbose = false;
    private final int maxStepDepth = 4; // TODO change back to 4
    private final int maxFanOut = 2;
    private final int numberOfEventProcessorThreads = 10;
    //private final List<ModelPathStepType> stepTypes = new ArrayList<>(Arrays.asList(ModelPathStepType.backRefs, ModelPathStepType.value));
    private final List<ModelPathStepType> stepTypes = new ArrayList<>(Arrays.asList(ModelPathStepType.values()));
    private final Executor executor = Executors.newFixedThreadPool(32);
    private final OrderIdProviderGenerator orderIdProviderGenerator = new OrderIdProviderGenerator();
    private final TenantId tenantId = new TenantId("test");
    private final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
    private final Id actorId = new Id(100000000);
    private final ModelPathGenerator pathGenerator = new ModelPathGenerator();
    private final EventFireGenerator eventFireGenerator = new EventFireGenerator(tenantId, actorId);


    @BeforeClass
    public void logger() {


        Logger rootLogger = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        LoggerContext loggerContext = rootLogger.getLoggerContext();
        loggerContext.reset();

        if (verbose) {

            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(loggerContext);
            encoder.setPattern("[%thread]: %message%n");
            encoder.start();

            ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
            appender.setContext(loggerContext);
            appender.setEncoder(encoder);
            appender.start();

            rootLogger.addAppender(appender);

            ((Logger)LoggerFactory.getLogger("com.jivesoftware.os.tasmo")).setLevel(Level.TRACE);
            ((Logger)LoggerFactory.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange")).setLevel(Level.TRACE);
            ((Logger)LoggerFactory.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore")).setLevel(Level.TRACE);
            ((Logger)LoggerFactory.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore")).setLevel(Level.TRACE);
        } else {

            rootLogger.setLevel(Level.OFF);
        }
    }

    @Test(dataProvider = "totalOrderAdds", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsOnly(AssertableCase inputCase)
            throws Throwable {
        inputCase.materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
        new AssertInputCase(executor, seed, tenantIdAndCentricId, actorId, maxFanOut, verbose).assertCombination(inputCase, null, true);
        inputCase.materialization.shutdown();
    }

    @Test(dataProvider = "addsThenRemoves", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsThenRemoves(AssertableCase inputCase)
            throws Throwable {
        inputCase.materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
        new AssertInputCase(executor, seed, tenantIdAndCentricId, actorId, maxFanOut, verbose).assertCombination(inputCase, null, true);
        inputCase.materialization.shutdown();
    }

    @Test(dataProvider = "addsThenRemovesThenAdds", invocationCount = 1, singleThreaded = true)
    public void testMultiThreadedAddsThenRemovesThenAdds(AssertableCase inputCase)
            throws Throwable {
        inputCase.materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
        new AssertInputCase(executor, seed, tenantIdAndCentricId, actorId, maxFanOut, verbose).assertCombination(inputCase, null, true);
        inputCase.materialization.shutdown();
    }

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
//                try {
//                    materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
//                } catch (Exception x) {
//                    throw new RuntimeException("Failed to setupModelAndMaterializer()" + x);
//                }
                idProvider = monatomic(highestId);
                EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);
                Set<Id> deletedIds = new HashSet<>();

                // Case: Entirely new adds
                EventFire eventFire = new EventFire(viewId,
                        deriedEventsAndViewId.getEvents(),
                        path.getPathMembers().get(path.getPathMemberSize() - 1),
                        deriedEventsAndViewId.getIdTree());

                Object[] buildParamaterListItem = buildParamaterListItem("adds",
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
//                        try {
//                            materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
//                        } catch (Exception x) {
//                            throw new RuntimeException("Failed to setupModelAndMaterializer()" + x);
//                        }
                        EventWriterProvider writerProvider = buildEventWriterProvider(materialization, idProvider);
                        EventFire eventFire = new EventFire(viewId,
                                deriedEventsAndViewId.getEvents(),
                                path.getPathMembers().get(path.getPathMemberSize() - 1),
                                deriedEventsAndViewId.getIdTree());

                        Object[] buildParamaterListItem = buildParamaterListItem("addsRandomOrder",
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
//                        materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
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

                        paramList.add(buildParamaterListItem("removes",
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
//                        materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
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

                        paramList.add(buildParamaterListItem("removes",
                                testId, materialization, tenantIdAndCentricId, actorId, binding, path, writerProvider, eventFire, new HashSet<Id>()));
                        testId++;
                    }
                }
            }
        }

        return paramList.iterator();
    }

    public List<ViewBinding> buildBindings(List<ModelPathStepType> refTypes, int maxNumSteps) throws Exception {

        List<String> pathStrings = pathGenerator.generateModelPaths(refTypes, maxNumSteps);
        List<ViewBinding> allViewBindings = Materialization.parseModelPathStrings(false, pathStrings);

        if (allViewBindings.size() > 1) {
            throw new IllegalStateException("Unexpectedly generated model paths with more than one view class name");
        }
        return allViewBindings;
    }

    private EventWriterProvider buildEventWriterProvider(Materialization materialization, OrderIdProvider idProvider) {
        JsonEventWriter jsonEventWriter = materialization.jsonEventWriter(materialization, idProvider);
        final EventWriter writer = new EventWriter(jsonEventWriter);

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
        return new Object[]{new AssertableCase(category,
            testId,
            materialization,
            tenantIdAndCentricId, actorId,
            new ViewBinding(binding.getViewClassName(), Arrays.asList(path), false, false, false, null),
            writerProvider,
            eventFire,
            deletedIds)};
    }

}

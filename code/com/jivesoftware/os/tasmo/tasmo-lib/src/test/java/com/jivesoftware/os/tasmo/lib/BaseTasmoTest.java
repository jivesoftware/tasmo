/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProvider;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventFieldValueType;
import com.jivesoftware.os.tasmo.model.EventsModel;
import com.jivesoftware.os.tasmo.model.TenantEventsProvider;
import com.jivesoftware.os.tasmo.model.VersionedEventsModel;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 *
 */
public class BaseTasmoTest {

    public static final TenantId MASTER_TENANT_ID = new TenantId("master");
    IdProvider idProvider;
    OrderIdProvider orderIdProvider;
    TenantId tenantId;
    TenantIdAndCentricId tenantIdAndCentricId;
    Id actorId;
    //UserIdentity userIdentity;
    EventWriter writer;
    ExistenceStore existenceStore;
    EventValueStore eventValueStore;
    ReferenceStore referenceStore;
    DispatcherProvider dispatcherProvider;
    TasmoViewMaterializer materializer;
    ChainedVersion currentVersion = new ChainedVersion("0", "1");
    AtomicReference<VersionedEventsModel> events = new AtomicReference<>();
    TenantEventsProvider eventsProvider;
    boolean useHBase = false;
    ObjectMapper mapper = new ObjectMapper();

    {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    }
    WrittenEventProvider<ObjectNode, JsonNode> eventProvider = new JsonWrittenEventProvider();
    private static BaseTasmoTest base;

    public static BaseTasmoTest create() {
        if (base == null) {
            base = new BaseTasmoTest();
        }
        return base;
    }

    public RowColumnValueStoreProvider getRowColumnValueStoreProvider(final String env) {
//        if (useHBase) {
//            final SetOfSortedMapsImplInitializer<Exception> hBase = new HBaseSetOfSortedMapsImplInitializer(
//                embeddedHBase.getConfiguration());
//            return new RowColumnValueStoreProvider() {
//
//                @Override
//                public RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore() throws IOException {
//                    return new NeverAcceptsFailureSetOfSortedMaps<>(hBase.initialize(env, "existenceTable", "v",
//                        new DefaultRowColumnValueStoreMarshaller<>(new TenantIdMarshaller(),
//                        new ObjectIdMarshaller(), new StringTypeMarshaller(),
//                        new StringTypeMarshaller()), new CurrentTimestamper()));
//                }
//
//                @Override
//                public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() throws IOException {
//                    return new NeverAcceptsFailureSetOfSortedMaps<>(hBase.initialize(env, "eventValueTable", "v",
//                        new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
//                        new ObjectIdMarshaller(), new StringTypeMarshaller(),
//                        eventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));
//                }
//
//                @Override
//                public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, String, RuntimeException> viewValueStore()
//                    throws IOException {
//                    return new NeverAcceptsFailureSetOfSortedMaps<>(hBase.initialize(env, "viewValueTable", "v",
//                        new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
//                        new ImmutableByteArrayMarshaller(), new ImmutableByteArrayMarshaller(), new StringTypeMarshaller()),
//                        new CurrentTimestamper()));
//                }
//
//                @Override
//                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks()
//                    throws IOException {
//                    return new NeverAcceptsFailureSetOfSortedMaps<>(hBase.initialize(env, "multiLinkTable", "v",
//                        new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
//                        new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));
//                }
//
//                @Override
//                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks()
//                    throws IOException {
//                    return new NeverAcceptsFailureSetOfSortedMaps<>(hBase.initialize(env, "multiBackLinkTable", "v",
//                        new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
//                        new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));
//                }
//            };
//
//        } else {
        return new RowColumnValueStoreProvider() {
            @Override
            public RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore() {
                return new RowColumnValueStoreImpl<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() {
                return new RowColumnValueStoreImpl<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, String, RuntimeException> viewValueStore() {
                return new RowColumnValueStoreImpl<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() {
                return new RowColumnValueStoreImpl<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() {
                return new RowColumnValueStoreImpl<>();
            }
        };
        //}
    }

    public static interface RowColumnValueStoreProvider {

        RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, String, RuntimeException> viewValueStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() throws Exception;
    }
    //private static EmbeddedHBase embeddedHBase = new EmbeddedHBase();

    @BeforeClass
    public void startHBase() throws Exception {
        if (useHBase) {
            //embeddedHBase.start(true);
        }
    }

    @AfterClass
    public void stopHBase() throws Exception {
        if (useHBase) {
            //embeddedHBase.stop();
        }
    }

    @BeforeClass
    public void setupPrimordialStuff() {
        orderIdProvider = idProvider();
        idProvider = new IdProviderImpl(orderIdProvider);
        tenantId = new TenantId("test");
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        actorId = new Id(1L);
    }

    @BeforeMethod
    public void setupModelAndMaterializer() throws Exception {

        String uuid = UUID.randomUUID().toString();

        RowColumnValueStoreProvider rowColumnValueStoreProvider = getRowColumnValueStoreProvider(uuid);
        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore = rowColumnValueStoreProvider.eventStore();
        RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStorage = rowColumnValueStoreProvider.existenceStore();

        existenceStore = new ExistenceStore(existenceStorage);
        eventValueStore = new EventValueStore(eventStore);

        referenceStore = new ReferenceStore(rowColumnValueStoreProvider.multiLinks(),
            rowColumnValueStoreProvider.multiBackLinks());

        TasmoEventBookkeeper tasmoEventBookkeeper = new TasmoEventBookkeeper(
            new CallbackStream<List<BookkeepingEvent>>() {
            @Override
            public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                return value;
            }
        });

        eventsProvider = new TenantEventsProvider(MASTER_TENANT_ID, null) {
            @Override
            public VersionedEventsModel getVersionedEventsModel(TenantId tenantId) {
                return events.get();
            }
        };


        dispatcherProvider = new DispatcherProvider(
            eventsProvider,
            referenceStore,
            eventValueStore);

        materializer = new TasmoViewMaterializer(tasmoEventBookkeeper,
            dispatcherProvider, existenceStore);

        writer = new EventWriter(jsonEventWriter(materializer, orderIdProvider));
    }
    //todo ref set/remove needs work

    Expectations initEventModel(String eventsModel) throws Exception {
        StringTokenizer tokenizer = new StringTokenizer(eventsModel, "|");
        EventsModel model = new EventsModel();

        while (tokenizer.hasMoreTokens()) {
            String eventDef = tokenizer.nextToken();
            String[] nameAndFields = eventDef.split(":");
            if (nameAndFields.length != 2) {
                throw new IllegalArgumentException();
            }

            Map<String, EventFieldValueType> fields = new HashMap<>();

            for (String fieldDef : nameAndFields[1].split(",")) {
                int idx = fieldDef.indexOf("(");
                if (idx < 0 || !fieldDef.endsWith(")")) {
                    throw new IllegalArgumentException("Field definitions require the form name(type)");
                }
                String fieldName = fieldDef.substring(0, idx);
                String fieldType = fieldDef.substring(idx + 1, fieldDef.indexOf(")"));

                fields.put(fieldName, EventFieldValueType.valueOf(fieldType));
            }

            model.addEvent(new EventDefinition(nameAndFields[0], fields));
        }

        return initEventModel(model);
    }

    Expectations initEventModel(EventsModel eventsModel) throws Exception {
        VersionedEventsModel newEvents = new VersionedEventsModel(currentVersion, eventsModel);
        events.set(newEvents);

        return new Expectations(eventProvider, eventValueStore, referenceStore, existenceStore);

    }

    OrderIdProvider idProvider() {
        return new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.addAndGet(2); // Have to move by twos so there is room for add vs remove differentiation.
            }
        };
    }

//    UserIdentity whoami() {
//        return new UserIdentity(new Id(1L));
//    }
    JsonEventWriter jsonEventWriter(final TasmoViewMaterializer tasmoViewMaterializer, final OrderIdProvider idProvider) {
        return new JsonEventWriter() {
            JsonEventConventions jsonEventConventions = new JsonEventConventions();

            @Override
            public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException {
                try {
                    List<ObjectId> objectIds = Lists.newArrayList();
                    List<Long> eventIds = Lists.newArrayList();
                    for (ObjectNode w : events) {
                        long eventId = idProvider.nextId();
                        eventIds.add(eventId);
                        jsonEventConventions.setEventId(w, eventId);

                        String instanceClassname = jsonEventConventions.getInstanceClassName(w);
                        ObjectId objectId = new ObjectId(instanceClassname, jsonEventConventions.getInstanceId(w, instanceClassname));
                        objectIds.add(objectId);

                    }

                    List<EventWrite> writtenEvents = new ArrayList<>();
                    for (ObjectNode eventNode : events) {
                        EventWrite eventWrite = new EventWrite(eventProvider.convertEvent(eventNode));
                        writtenEvents.add(eventWrite);
                    }

                    tasmoViewMaterializer.process(writtenEvents);

                    return new EventWriterResponse(eventIds, objectIds);

                } catch (Exception ex) {
                    throw new JsonEventWriteException("sad trombone", ex);
                }
            }
        };
    }

    ObjectId write(Event event) throws EventWriteException {
        EventWriterResponse eventWriterResponse = writer.write(event);
        return eventWriterResponse.getObjectIds().get(0);
    }
}

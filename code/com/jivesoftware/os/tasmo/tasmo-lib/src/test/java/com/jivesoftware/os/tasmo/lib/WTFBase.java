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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdProvider;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.configuration.BindingGenerator;
import com.jivesoftware.os.tasmo.configuration.ViewModel;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.lib.TasmoServiceInitializer.TasmoServiceConfig;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.MurmurHashViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.AllAllowedViewPermissionsChecker;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.StaleViewFieldStream;
import com.jivesoftware.os.tasmo.view.reader.service.ViewAsObjectNode;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import com.jivesoftware.os.tasmo.view.reader.service.ViewProvider;
import com.jivesoftware.os.tasmo.view.reader.service.ViewValueReader;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 *
 */
public class WTFBase {

    public static final TenantId MASTER_TENANT_ID = new TenantId("master");
    IdProvider idProvider;
    OrderIdProvider orderIdProvider;
    TenantId tenantId;
    TenantIdAndCentricId tenantIdAndCentricId;
    Id actorId;
    EventWriter writer;
    ViewValueStore viewValueStore;
    ViewValueWriter viewValueWriter;
    ViewValueReader viewValueReader;
    ViewPermissionChecker viewPermissionChecker;
    ViewProvider<ViewResponse> viewProvider;
    TasmoViewModel tasmoViewModel;
    final ChainedVersion currentVersion = new ChainedVersion("0", "1");
    final AtomicReference<Views> views = new AtomicReference<>();
    ViewsProvider viewsProvider;
    ObjectMapper mapper = new ObjectMapper();
    TasmoModelFactory tasmoModelFactory = new TasmoModelFactory();

    {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    }
    WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();
    private static WTFBase base;

    public static WTFBase create() {
        if (base == null) {
            base = new WTFBase();
        }
        return base;
    }

    public ViewChangeNotificationProcessor getViewChangeNotificationProcessor() {
        // default is a no op processor
        return new ViewChangeNotificationProcessor() {
            @Override
            public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
            }
        };
    }

    public RowColumnValueStoreProvider getRowColumnValueStoreProvider(final String env) {

            return new RowColumnValueStoreProvider() {
                RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore = new RowColumnValueStoreImpl<>();
                RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore = new RowColumnValueStoreImpl<>();
                RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks = new RowColumnValueStoreImpl<>();
                RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks = new RowColumnValueStoreImpl<>();

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() {
                    return eventStore;
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore() {
                    return viewValueStore;
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() {
                    return multiLinks;
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() {
                    return multiBackLinks;
                }
            };

    }

    public static interface RowColumnValueStoreProvider {

        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() throws Exception;
    }


    @BeforeMethod
    public void setupModelAndMaterializer() throws Exception {

        orderIdProvider = idProvider();
        idProvider = new IdProviderImpl(orderIdProvider);
        tenantId = new TenantId("test");
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        actorId = new Id(1L);

        String uuid = UUID.randomUUID().toString();
        ViewPathKeyProvider  keyProvider = new MurmurHashViewPathKeyProvider();
        viewPermissionChecker = new AllAllowedViewPermissionsChecker();

        final RowColumnValueStoreProvider rowColumnValueStoreProvider = getRowColumnValueStoreProvider(uuid);
        final RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrency = new RowColumnValueStoreImpl<>();
        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(concurrency);

        viewValueStore = new ViewValueStore(rowColumnValueStoreProvider.viewValueStore(), new MurmurHashViewPathKeyProvider());
        viewValueWriter = new ViewValueWriter(viewValueStore);
        viewValueReader = new ViewValueReader(viewValueStore);

        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, rowColumnValueStoreProvider.multiLinks(),
            rowColumnValueStoreProvider.multiBackLinks());

        CommitChange commitChange = TasmoMaterializerHarnessFactory.createCommitToViewValueStore(rowColumnValueStoreProvider.viewValueStore(), keyProvider);

        viewsProvider = new ViewsProvider() {
            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                return views.get();
            }

            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return currentVersion;
            }
        };

        tasmoViewModel = new TasmoViewModel(MASTER_TENANT_ID,
            viewsProvider,
            new MurmurHashViewPathKeyProvider(),
            referenceStore);

        TasmoServiceConfig serviceConfig = BindInterfaceToConfiguration.bindDefault(TasmoServiceConfig.class);

        final TasmoEventIngress tasmoEventIngress = TasmoServiceInitializer.initializeEventIngressCallbackStream(orderIdProvider,
            viewsProvider,
            new MurmurHashViewPathKeyProvider(),
            writtenEventProvider,
            new TasmoStorageProvider() {

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStorage() throws Exception {
                    return rowColumnValueStoreProvider.eventStore();
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrencyStorage() throws Exception {
                    return concurrency;
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage() throws Exception {
                    return rowColumnValueStoreProvider.viewValueStore();
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinksStorage() throws Exception {
                    return rowColumnValueStoreProvider.multiLinks();
                }

                @Override
                public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinksStorage() throws Exception {
                    return rowColumnValueStoreProvider.multiBackLinks();
                }
            },
            commitChange,
            getViewChangeNotificationProcessor(),
            new CallbackStream<List<BookkeepingEvent>>() {
                @Override
                public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                    return value;
                }
            },
            Optional.<WrittenEventProcessorDecorator>absent(),
            new TasmoBlacklist(),
            serviceConfig);


        writer = new EventWriter(jsonEventWriter(tasmoEventIngress, orderIdProvider));

    }

    @AfterMethod
    public void shutdownMaterializer() throws Exception {
    }

    void initModelPaths(ArrayNode views) throws Exception {
        List<ViewBinding> viewBindingsList = new LinkedList<>();
        BindingGenerator bindingGenerator = new BindingGenerator();

        for (JsonNode view : views) {
            ViewModel viewConfiguration = ViewModel.builder((ObjectNode) view).build();
            viewBindingsList.add(bindingGenerator.generate(null, viewConfiguration));
        }
        initModelPaths(viewBindingsList);
    }

    void initModelPaths(List<ViewBinding> bindings) throws Exception {
        Views newViews = new Views(tenantId, currentVersion, bindings);
        views.set(newViews);

        tasmoViewModel.loadModel(MASTER_TENANT_ID);

        TenantId masterTenantId = new TenantId("master");
        TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(masterTenantId, viewsProvider, new MurmurHashViewPathKeyProvider());
        tenantViewsProvider.loadModel(masterTenantId);

        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                System.out.println("Encountered stale fields for:" + viewDescriptor + " value:" + value);
            }
        };

        viewProvider = new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            new ViewAsObjectNode(),
            new JsonViewMerger(mapper),
            staleViewFieldStream,
            1024 * 1024 * 10);

    }



    void initModelPaths(String... simpleBindings) throws Exception {
        List<ViewBinding> viewBindingsList = tasmoModelFactory.parseModelPathStrings(simpleBindings);
        initModelPaths(viewBindingsList);
    }

    void initModelPaths(boolean idCentric, String... simpleBindings) throws Exception {
        List<ViewBinding> viewBindingsList = tasmoModelFactory.parseModelPathStrings(idCentric, simpleBindings);
        initModelPaths(viewBindingsList);
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
    JsonEventWriter jsonEventWriter(final TasmoEventIngress tasmoViewMaterializer, final OrderIdProvider idProvider) {
        return new JsonEventWriter() {
            JsonEventConventions jsonEventConventions = new JsonEventConventions();

            @Override
            public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException {
                try {
                    List<ObjectId> objectIds = Lists.newArrayList();
                    List<Long> eventIds = Lists.newArrayList();
                    for (ObjectNode w : events) {
                        long eventId = jsonEventConventions.getEventId(w);
                        if (eventId == 0) {
                            eventId = idProvider.nextId();
                            jsonEventConventions.setEventId(w, eventId);
                        }
                        eventIds.add(eventId);

                        String instanceClassname = jsonEventConventions.getInstanceClassName(w);
                        ObjectId objectId = new ObjectId(instanceClassname, jsonEventConventions.getInstanceId(w, instanceClassname));
                        objectIds.add(objectId);

                    }

                    List<WrittenEvent> writtenEvents = new ArrayList<>();
                    for (ObjectNode eventNode : events) {
                        writtenEvents.add(writtenEventProvider.convertEvent(eventNode));
                    }

                    List<WrittenEvent> failedToProcess = tasmoViewMaterializer.callback(writtenEvents);
                    while (!failedToProcess.isEmpty()) {
                        System.out.println("FAILED to process " + failedToProcess.size() + " events likely due to consistency issues.");
                        failedToProcess = tasmoViewMaterializer.callback(failedToProcess);
                    }

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

    List<ObjectId> write(List<Event> events) throws EventWriteException {
        EventWriterResponse eventWriterResponse = writer.write(events);
        return eventWriterResponse.getObjectIds();
    }

    protected ObjectNode readView(TenantIdAndCentricId tenantIdAndCentricId, Id actorId, ObjectId viewId) throws IOException {
        ViewResponse viewResponse = viewProvider.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        // kinda patch to refrain from refactoring dozens of tests... readView used to return null in case of a non-existing view
        return viewResponse.getStatusCode() == ViewResponse.StatusCode.OK ? viewResponse.getViewBody() : null;
    }


}

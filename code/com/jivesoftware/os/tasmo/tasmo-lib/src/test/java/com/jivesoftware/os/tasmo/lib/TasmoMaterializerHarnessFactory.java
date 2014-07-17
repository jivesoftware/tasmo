package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.lib.TasmoJITViewReadMaterializationInitializer.TasmoJITViewReadMaterializationConfig;
import com.jivesoftware.os.tasmo.lib.TasmoNotificationReadMaterializerInitializer.TasmoNotificationReadMaterializerConfig;
import com.jivesoftware.os.tasmo.lib.TasmoServiceInitializer.TasmoServiceConfig;
import com.jivesoftware.os.tasmo.lib.TasmoSyncWriteInitializer.TasmoSyncWriteConfig;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoEventIngress;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoNotificationsIngress;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializerViewFields;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.NoOpCommitChange;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.SyncEventWriter;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
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
import com.jivesoftware.os.tasmo.view.notification.api.NoOpViewNotificationListener;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotificationListener;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReadMaterializer;
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
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriteFieldChange;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriterException;
import com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.BindInterfaceToConfiguration;

/**
 *
 * @author jonathan.colt
 */
public class TasmoMaterializerHarnessFactory {

    static TasmoStorageProvider createInmemoryTasmoStorageProvider() {
        return new TasmoStorageProvider() {
            private final RowColumnValueStore<TenantId, Id, ObjectId, String, RuntimeException> modifierStorage = new RowColumnValueStoreImpl<>();
            private final RowColumnValueStore<TenantIdAndCentricId,
                ObjectId, String, OpaqueFieldValue, RuntimeException> eventStorage = new RowColumnValueStoreImpl<>();
            private final RowColumnValueStore<TenantIdAndCentricId,
                ObjectId, String, Long, RuntimeException> concurrencyStorage = new RowColumnValueStoreImpl<>();
            private final RowColumnValueStore<TenantIdAndCentricId,
                ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage = new RowColumnValueStoreImpl<>();
            private final RowColumnValueStore<TenantIdAndCentricId,
                ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinksStorage = new RowColumnValueStoreImpl<>();
            private final RowColumnValueStore<TenantIdAndCentricId,
                ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinksStorage = new RowColumnValueStoreImpl<>();

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStorage() throws Exception {
                return eventStorage;
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrencyStorage() throws Exception {
                return concurrencyStorage;
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage() throws
                Exception {
                return viewValueStorage;
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinksStorage() throws Exception {
                return multiLinksStorage;
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinksStorage() throws Exception {
                return multiBackLinksStorage;
            }

            @Override
            public RowColumnValueStore<TenantId, Id, ObjectId, String, RuntimeException> modifierStorage() throws
                Exception {
                return modifierStorage;
            }
        };
    }

    static NoOpEventBookkeeper createNoOpEventBookkeeper() {
        return new NoOpEventBookkeeper();
    }

    static class NoOpEventBookkeeper implements CallbackStream<List<BookkeepingEvent>> {

        @Override
        public List<BookkeepingEvent> callback(List<BookkeepingEvent> v) throws Exception {
            return v;
        }
    }

    static ViewChangeNotificationProcessor createNoOpViewChangeNotificationProcessor() {
        return new NoOpViewChangeNotificationProcessor();
    }

    static class NoOpViewChangeNotificationProcessor implements ViewChangeNotificationProcessor {

        @Override
        public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
        }
    }

    static ViewPermissionChecker createNoOpViewPermissionChecker() {
        return new AllAllowedViewPermissionsChecker();
    }

    static OrderIdProvider createOrderIdProvider() {
        return new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.addAndGet(2); // Have to move by twos so there is room for add vs remove differentiation.
            }
        };
    }

    static TasmoMaterializerHarness createWriteTimeMaterializer(final OrderIdProvider idProvider,
        final TasmoStorageProvider tasmoStorageProvider,
        final CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
        final ViewChangeNotificationProcessor changeNotificationProcessor,
        final ViewPermissionChecker viewPermissionChecker) throws Exception {

        final ChainedVersion currentVersion = new ChainedVersion("0", "1");
        final JsonViewMerger merger = new JsonViewMerger(new ObjectMapper());
        final ViewAsObjectNode viewAsObjectNode = new ViewAsObjectNode();
        final ViewPathKeyProvider pathKeyProvider = new MurmurHashViewPathKeyProvider();
        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();

        final AtomicReference<Views> views = new AtomicReference<>();
        ViewsProvider viewsProvider = new ViewsProvider() {
            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                return views.get();
            }

            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return currentVersion;
            }
        };

        final TasmoViewModelInitializer.TasmoViewModelConfig tasmoViewModelConfig = BindInterfaceToConfiguration
            .bindDefault(TasmoViewModelInitializer.TasmoViewModelConfig.class);
        TasmoServiceHandle<TasmoViewModel> tasmoViewModel = TasmoViewModelInitializer.initialize(viewsProvider, pathKeyProvider, tasmoViewModelConfig);
        tasmoViewModel.start();

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage = tasmoStorageProvider.
            viewValueStorage();
        ViewValueStore viewValueStore = new ViewValueStore(viewValueStorage, pathKeyProvider);

        CommitChange commitChange = createCommitToViewValueStore(viewValueStorage, pathKeyProvider);

        final TasmoBlacklist tasmoBlacklist = new TasmoBlacklist();

        TasmoServiceConfig serviceConfig = BindInterfaceToConfiguration.bindDefault(TasmoServiceConfig.class);

        final TasmoEventIngress tasmoEventIngress = TasmoServiceInitializer.initialize(idProvider,
            tasmoViewModel.getService(),
            writtenEventProvider,
            tasmoStorageProvider,
            commitChange,
            changeNotificationProcessor,
            new NoOpViewNotificationListener(),
            bookkeepingStream,
            Optional.<WrittenEventProcessorDecorator>absent(),
            tasmoBlacklist,
            serviceConfig);

        JsonEventWriter jsonEventWriter = jsonEventWriter(idProvider, writtenEventProvider, tasmoEventIngress);
        final EventWriter eventWriter = new EventWriter(jsonEventWriter);
        final ViewExpectations expectations = new ViewExpectations(viewValueStore, pathKeyProvider);

        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                System.out.println("Encountered stale fields for:" + viewDescriptor + " value:" + value);
            }
        };

        final TenantId masterTenantId = new TenantId("master");
        final TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(masterTenantId, viewsProvider, pathKeyProvider);

        ViewValueReader viewValueReader = new ViewValueReader(viewValueStore);
        final ViewProvider<ViewResponse> viewProvider = new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            viewAsObjectNode,
            merger,
            staleViewFieldStream,
            1_024 * 1_024 * 10);

        return new TasmoMaterializerHarness() {

            @Override
            public ObjectId write(Event event) throws EventWriteException {
                return eventWriter.write(event).getObjectIds().get(0);
            }

            @Override
            public List<ObjectId> write(List<Event> events) throws EventWriteException {
                return eventWriter.write(events).getObjectIds();
            }

            @Override
            public void addExpectation(ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value) {
                expectations.addExpectation(rootId, viewClassName, viewFieldName, pathIds, fieldName, value);
            }

            @Override
            public void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {
                expectations.assertExpectation(tenantIdAndCentricId);
            }

            @Override
            public void clearExpectations() {
                expectations.clear();
            }

            @Override
            public ObjectNode readView(TenantIdAndCentricId tenantIdAndCentricId, Id actorId, ObjectId viewId) throws Exception {
                ViewResponse view = viewProvider.readView(new ViewDescriptor(tenantIdAndCentricId.getTenantId(), actorId, viewId));
                if (view != null && view.hasViewBody()) {
                    return view.getViewBody();
                }
                return null;
            }

            @Override
            public IdProvider idProvider() {
                return new IdProvider() {

                    @Override
                    public Id nextId() {
                        return new Id(idProvider.nextId());
                    }
                };
            }

            @Override
            public void initModel(Views _views) {
                views.set(_views);
                tenantViewsProvider.loadModel(masterTenantId);
                expectations.init(_views);
            }

            @Override
            public String toString() {
                return "WriteMaterializeHarness";
            }
        };
    }

    static TasmoMaterializerHarness createSyncWriteSyncReadsMaterializer(final OrderIdProvider idProvider,
        final TasmoStorageProvider tasmoStorageProvider,
        final CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
        final ViewChangeNotificationProcessor changeNotificationProcessor,
        final ViewPermissionChecker viewPermissionChecker) throws Exception {

        final ChainedVersion currentVersion = new ChainedVersion("0", "1");
        final ViewPathKeyProvider pathKeyProvider = new MurmurHashViewPathKeyProvider();
        final AtomicReference<Views> views = new AtomicReference<>();
        final ViewsProvider viewsProvider = new ViewsProvider() {
            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                return views.get();
            }

            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return currentVersion;
            }
        };

        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();
        final TasmoBlacklist tasmoBlacklist = new TasmoBlacklist();

        final TasmoViewModelInitializer.TasmoViewModelConfig tasmoViewModelConfig = BindInterfaceToConfiguration
            .bindDefault(TasmoViewModelInitializer.TasmoViewModelConfig.class);
        TasmoServiceHandle<TasmoViewModel> tasmoViewModel = TasmoViewModelInitializer.initialize(viewsProvider, pathKeyProvider, tasmoViewModelConfig);
        tasmoViewModel.start();

        TasmoSyncWriteConfig syncWriteConfig = BindInterfaceToConfiguration.bindDefault(TasmoSyncWriteConfig.class);

        final SyncEventWriter syncEventWriter = TasmoSyncWriteInitializer.initialize(tasmoViewModel.getService(),
            writtenEventProvider,
            tasmoStorageProvider,
            bookkeepingStream,
            tasmoBlacklist,
            syncWriteConfig);

        JsonEventWriter jsonEventWriter = jsonEventWriter(idProvider, writtenEventProvider, syncEventWriter);
        final EventWriter eventWriter = new EventWriter(jsonEventWriter);

        final TasmoJITViewReadMaterializationConfig viewReadMaterializationConfig = BindInterfaceToConfiguration.bindDefault(
            TasmoJITViewReadMaterializationConfig.class);

        return new TasmoMaterializerHarness() {

            ViewReadMaterializer<ViewResponse> viewReadMaterializer;
            ViewExpectations expectations;
            Views lastViews;

            public void reset() throws Exception {
                RowColumnValueStore<TenantIdAndCentricId,
                    ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage = new RowColumnValueStoreImpl<>();

                CommitChange commitChangeVistor = createCommitToViewValueStore(viewValueStorage, pathKeyProvider);

                final TasmoViewModelInitializer.TasmoViewModelConfig tasmoViewModelConfig = BindInterfaceToConfiguration
                    .bindDefault(TasmoViewModelInitializer.TasmoViewModelConfig.class);
                TasmoServiceHandle<TasmoViewModel> tasmoViewModel = TasmoViewModelInitializer.initialize(viewsProvider, pathKeyProvider, tasmoViewModelConfig);
                tasmoViewModel.start();

                final TasmoReadMaterializerInitializer.TasmoReadMaterializerConfig readMaterializationConfig = BindInterfaceToConfiguration
                    .bindDefault(TasmoReadMaterializerInitializer.TasmoReadMaterializerConfig.class);
                TasmoServiceHandle<ReadMaterializerViewFields> readMateriaizer = TasmoReadMaterializerInitializer.initialize(readMaterializationConfig,
                    tasmoViewModel.getService(), writtenEventProvider, tasmoStorageProvider);
                readMateriaizer.start();

                TasmoServiceHandle<ViewReadMaterializer<ViewResponse>> readMaterialization = TasmoJITViewReadMaterializationInitializer
                    .initialize(viewReadMaterializationConfig, tasmoViewModel.getService(), readMateriaizer.getService(),
                        viewPermissionChecker, Optional.of(commitChangeVistor));

                viewReadMaterializer = readMaterialization.getService();

                ViewValueStore viewValueStore = new ViewValueStore(viewValueStorage, pathKeyProvider);
                expectations = new ViewExpectations(viewValueStore, pathKeyProvider);
                if (lastViews != null) {
                    expectations.init(lastViews);
                }
            }

            @Override
            public ObjectId write(Event event) throws EventWriteException {
                return eventWriter.write(event).getObjectIds().get(0);
            }

            @Override
            public List<ObjectId> write(List<Event> events) throws EventWriteException {
                return eventWriter.write(events).getObjectIds();
            }

            @Override
            public void addExpectation(ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value) {
                expectations.addExpectation(rootId, viewClassName, viewFieldName, pathIds, fieldName, value);
            }

            @Override
            public void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {
                expectations.assertExpectation(tenantIdAndCentricId);
            }

            @Override
            public void clearExpectations() throws Exception {
                reset();
            }

            @Override
            public ObjectNode readView(TenantIdAndCentricId tenantIdAndCentricId, Id actorId, ObjectId viewId) throws Exception {
                ViewResponse view = viewReadMaterializer.readMaterializeView(new ViewDescriptor(tenantIdAndCentricId.getTenantId(), actorId, viewId));
                if (view != null && view.hasViewBody()) {
                    return view.getViewBody();
                }
                return null;
            }

            @Override
            public IdProvider idProvider() {
                return new IdProvider() {

                    @Override
                    public Id nextId() {
                        return new Id(idProvider.nextId());
                    }
                };
            }

            @Override
            public void initModel(Views _views) throws Exception {
                lastViews = _views;
                reset();
                views.set(_views);
            }

            @Override
            public String toString() {
                return "SyncWriteSyncReadHarness";
            }

        };

    }

    static TasmoMaterializerHarness createSynWriteNotificationReadMaterializer(final OrderIdProvider idProvider,
        final TasmoStorageProvider asyncTasmoStorageProvider,
        final TasmoStorageProvider syncTasmoStorageProvider,
        final CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
        final ViewPermissionChecker viewPermissionChecker) throws Exception {

        final ChainedVersion currentVersion = new ChainedVersion("0", "1");
        final ViewPathKeyProvider pathKeyProvider = new MurmurHashViewPathKeyProvider();
        final JsonViewMerger merger = new JsonViewMerger(new ObjectMapper());
        final ViewAsObjectNode viewAsObjectNode = new ViewAsObjectNode();
        final AtomicReference<Views> views = new AtomicReference<>();
        final ViewsProvider viewsProvider = new ViewsProvider() {
            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                return views.get();
            }

            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return currentVersion;
            }
        };

        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();
        final TasmoBlacklist tasmoBlacklist = new TasmoBlacklist();

        final TasmoViewModelInitializer.TasmoViewModelConfig tasmoViewModelConfig = BindInterfaceToConfiguration
            .bindDefault(TasmoViewModelInitializer.TasmoViewModelConfig.class);
        final TasmoServiceHandle<TasmoViewModel> tasmoViewModel = TasmoViewModelInitializer.initialize(viewsProvider, pathKeyProvider, tasmoViewModelConfig);
        tasmoViewModel.start();

        TasmoSyncWriteConfig syncWriteConfig = BindInterfaceToConfiguration.bindDefault(TasmoSyncWriteConfig.class);

        final SyncEventWriter syncEventWriter = TasmoSyncWriteInitializer.initialize(tasmoViewModel.getService(),
            writtenEventProvider,
            syncTasmoStorageProvider,
            bookkeepingStream,
            tasmoBlacklist,
            syncWriteConfig);

        RowColumnValueStore<TenantIdAndCentricId,
            ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage = syncTasmoStorageProvider.viewValueStorage();
        ViewValueStore viewValueStore = new ViewValueStore(viewValueStorage, pathKeyProvider);

        final TasmoReadMaterializerInitializer.TasmoReadMaterializerConfig readMaterializationConfig = BindInterfaceToConfiguration
            .bindDefault(TasmoReadMaterializerInitializer.TasmoReadMaterializerConfig.class);
        TasmoServiceHandle<ReadMaterializerViewFields> readMateriaizer = TasmoReadMaterializerInitializer.initialize(readMaterializationConfig,
            tasmoViewModel.getService(), writtenEventProvider, syncTasmoStorageProvider);
        readMateriaizer.start();

        TasmoNotificationReadMaterializerConfig notificationServiceConfig = BindInterfaceToConfiguration.bindDefault(
            TasmoNotificationReadMaterializerConfig.class);
        TasmoServiceHandle<TasmoNotificationsIngress> notificationIngressHandle = TasmoNotificationReadMaterializerInitializer.initialize(
            notificationServiceConfig,
            idProvider,
            readMateriaizer.getService(),
            new ViewValueWriter(viewValueStore));

        notificationIngressHandle.start();
        final TasmoNotificationsIngress notificationIngress = notificationIngressHandle.getService();

        ViewChangeNotificationProcessor notificationProcessor = new ViewChangeNotificationProcessor() {

            @Override
            public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
            }
        };

        ViewNotificationListener allViewNotificationListener = new ViewNotificationListener() {

            @Override
            public void handleNotifications(List<ViewNotification> viewNotifications) throws Exception {
                System.out.println("notifications:" + viewNotifications);
                List<ViewNotification> failedToProcess = notificationIngress.callback(viewNotifications);
                while (!failedToProcess.isEmpty()) {
                    System.out.println("FAILED to process notifications " + failedToProcess.size() + " events likely due to consistency issues.");
                    failedToProcess = notificationIngress.callback(failedToProcess);
                }
            }

            @Override
            public void flush() {
            }
        };

        CommitChange commitChange = new NoOpCommitChange();
        //CommitChange commitChange = createCommitToViewValueStore(viewValueStorage, pathKeyProvider);
        TasmoServiceConfig serviceConfig = BindInterfaceToConfiguration.bindDefault(TasmoServiceConfig.class);
        final TasmoEventIngress tasmoEventIngress = TasmoServiceInitializer.initialize(idProvider,
            tasmoViewModel.getService(),
            writtenEventProvider,
            asyncTasmoStorageProvider,
            commitChange,
            notificationProcessor,
            allViewNotificationListener,
            bookkeepingStream,
            Optional.<WrittenEventProcessorDecorator>absent(),
            tasmoBlacklist,
            serviceConfig);

        JsonEventWriter jsonEventWriter = jsonEventWriter(idProvider, writtenEventProvider, syncEventWriter, tasmoEventIngress);
        final EventWriter eventWriter = new EventWriter(jsonEventWriter);

        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                System.out.println("Encountered stale fields for:" + viewDescriptor + " value:" + value);
            }
        };

        final TenantId masterTenantId = new TenantId("master");
        final TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(masterTenantId, viewsProvider, pathKeyProvider);

        final ViewExpectations expectations = new ViewExpectations(viewValueStore, pathKeyProvider);
        ViewValueReader viewValueReader = new ViewValueReader(viewValueStore);
        final ViewProvider<ViewResponse> viewProvider = new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            viewAsObjectNode,
            merger,
            staleViewFieldStream,
            1_024 * 1_024 * 10);

        return new TasmoMaterializerHarness() {

            @Override
            public void initModel(Views _views) throws Exception {
                views.set(_views);
                tasmoViewModel.getService().loadModel(masterTenantId);
                tenantViewsProvider.loadModel(masterTenantId);
                expectations.init(_views);
            }

            @Override
            public IdProvider idProvider() {
                return new IdProvider() {

                    @Override
                    public Id nextId() {
                        return new Id(idProvider.nextId());
                    }
                };
            }

            @Override
            public ObjectId write(Event event) throws EventWriteException {
                return eventWriter.write(event).getObjectIds().get(0);
            }

            @Override
            public List<ObjectId> write(List<Event> events) throws EventWriteException {
                return eventWriter.write(events).getObjectIds();
            }

            @Override
            public void addExpectation(ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value) {
                expectations.addExpectation(rootId, viewClassName, viewFieldName, pathIds, fieldName, value);
            }

            @Override
            public void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {
                expectations.assertExpectation(tenantIdAndCentricId);
            }

            @Override
            public void clearExpectations() throws Exception {
                expectations.clear();
            }

            @Override
            public ObjectNode readView(TenantIdAndCentricId tenantIdAndCentricId, Id actorId, ObjectId viewId) throws Exception {
                ViewResponse view = viewProvider.readView(new ViewDescriptor(tenantIdAndCentricId.getTenantId(), actorId, viewId));
                if (view != null && view.hasViewBody()) {
                    return view.getViewBody();
                }
                return null;
            }
        };
    }
   
    public static CommitChange createCommitToViewValueStore(
        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage,
        final ViewPathKeyProvider pathKeyProvider) {

        ViewValueStore viewValueStore = new ViewValueStore(viewValueStorage, pathKeyProvider);
        ViewValueWriter viewValueWriter = new ViewValueWriter(viewValueStore);
        final WriteToViewValueStore writeToViewValueStore = new WriteToViewValueStore(viewValueWriter);
        CommitChange commitChange = new CommitChange() {
            @Override
            public void commitChange(WrittenEventContext batchContext,
                TenantIdAndCentricId tenantIdAndCentricId,
                List<ViewField> changes) throws CommitChangeException {
                List<ViewWriteFieldChange> write = new ArrayList<>(changes.size());
                for (ViewField change : changes) {
                    try {
                        PathId[] modelPathInstanceIds = change.getModelPathInstanceIds();
                        ObjectId[] ids = new ObjectId[modelPathInstanceIds.length];
                        for (int i = 0; i < ids.length; i++) {
                            ids[i] = modelPathInstanceIds[i].getObjectId();
                        }
                        ViewWriteFieldChange viewWriteFieldChange = new ViewWriteFieldChange(
                            change.getEventId(),
                            tenantIdAndCentricId,
                            change.getActorId(),
                            ViewWriteFieldChange.Type.valueOf(change.getType().name()),
                            change.getViewObjectId(),
                            change.getModelPathIdHashcode(),
                            ids,
                            new ViewValue(change.getModelPathTimestamps(), change.getValue()),
                            change.getTimestamp());
                        write.add(viewWriteFieldChange);
                    } catch (Exception ex) {
                        throw new CommitChangeException("Failed to add change for the following reason.", ex);
                    }
                }

                try {
                    writeToViewValueStore.write(tenantIdAndCentricId, write);
                } catch (ViewWriterException ex) {
                    throw new CommitChangeException("Failed to write BigInteger?", ex);
                }
            }
        };
        return commitChange;
    }

    static JsonEventWriter jsonEventWriter(final OrderIdProvider idProvider,
        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider,
        final CallbackStream<List<WrittenEvent>>... tasmoEventIngress) {

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

                    for (CallbackStream<List<WrittenEvent>> ingress : tasmoEventIngress) {
                        List<WrittenEvent> failedToProcess = ingress.callback(writtenEvents);
                        while (!failedToProcess.isEmpty()) {
                            System.out.println("FAILED to process " + failedToProcess.size() + " events likely due to consistency issues.");
                            failedToProcess = ingress.callback(failedToProcess);
                        }
                    }

                    return new EventWriterResponse(eventIds, objectIds);

                } catch (Exception ex) {
                    throw new JsonEventWriteException("sad trombone", ex);
                }
            }
        };
    }
}

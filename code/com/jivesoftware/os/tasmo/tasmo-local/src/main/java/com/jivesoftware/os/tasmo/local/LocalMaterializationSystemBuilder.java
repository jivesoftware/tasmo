package com.jivesoftware.os.tasmo.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.lib.read.StatCollectingFieldValueReader;
import com.jivesoftware.os.tasmo.lib.TasmoBlacklist;
import com.jivesoftware.os.tasmo.lib.process.TasmoEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.TasmoEventTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.TasmoEventTraverser;
import com.jivesoftware.os.tasmo.lib.process.ProcessingStats;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoWriteMaterializer;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.WriteFanoutEventPersistor;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.lib.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.MurmurHashViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.BatchingReferenceTraverser;
import com.jivesoftware.os.tasmo.view.notification.api.NoOpViewNotificationListener;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.StaleViewFieldStream;
import com.jivesoftware.os.tasmo.view.reader.service.ViewAsObjectNode;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionCheckResult;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import com.jivesoftware.os.tasmo.view.reader.service.ViewProvider;
import com.jivesoftware.os.tasmo.view.reader.service.ViewValueReader;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriteFieldChange;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriterException;
import com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 *
 */
public class LocalMaterializationSystemBuilder implements LocalMaterializationSystem.ShutdownCallback {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private RowColumnValueStoreProvider rowColumnValueStoreProvider;
    private ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private OrderIdProvider orderIdProvider;
    private final ViewPathKeyProvider viewPathKeyProvider = new MurmurHashViewPathKeyProvider();

    public LocalMaterializationSystemBuilder setViewChangeNotificationProcessor(ViewChangeNotificationProcessor viewChangeNotificationProcessor) {
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        return this;
    }

    public LocalMaterializationSystemBuilder setOrderIdProvider(OrderIdProvider provider) {
        this.orderIdProvider = provider;
        return this;
    }

    public LocalMaterializationSystem build(List<ViewBinding> viewDefinitions) throws Exception {
        TenantId masterTenantId = new TenantId("ephemeral");

        ViewsProvider viewsProvider = buildViewsProvider(masterTenantId, viewDefinitions);

        WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();

        RowColumnValueStoreUtil rowColumnValueStoreUtil = new RowColumnValueStoreUtil();

        String uuid = UUID.randomUUID().toString();

        rowColumnValueStoreProvider = rowColumnValueStoreUtil.getInMemoryRowColumnValueStoreProvider(uuid, writtenEventProvider);

        ViewValueStore viewValueStore = buildViewValueStore(rowColumnValueStoreProvider, viewPathKeyProvider);
        CommitChange commitChange = buildCommitChange(viewValueStore);
        TasmoWriteMaterializer viewMaterializer = buildViewMaterializer(viewsProvider, rowColumnValueStoreProvider,
            writtenEventProvider, commitChange, masterTenantId);

        EventWriter eventWriter = buildEventWriter(viewMaterializer, writtenEventProvider);
        ViewReader<ViewResponse> viewReader = buildViewReader(viewValueStore, viewsProvider, masterTenantId);

        return new LocalMaterializationSystem(eventWriter, viewReader, orderIdProvider, this);
    }

    private ConcurrencyStore buildConcurrencyStore(RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new HBaseBackedConcurrencyStore(rowColumnValueStoreProvider.concurrencyStore());
    }

    private ReferenceStore buildReferenceStore(ConcurrencyStore concurrencyStore, RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new ReferenceStore(concurrencyStore, rowColumnValueStoreProvider.multiLinks(), rowColumnValueStoreProvider.multiBackLinks());
    }

    private EventValueStore buildEventValueStore(ConcurrencyStore concurrencyStore, RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new EventValueStore(concurrencyStore, rowColumnValueStoreProvider.eventStore());
    }

    private ViewValueStore buildViewValueStore(RowColumnValueStoreProvider rowColumnValueStoreProvider,
        ViewPathKeyProvider viewPathKeyProvider) throws Exception {
        return new ViewValueStore(rowColumnValueStoreProvider.viewValueStore(), viewPathKeyProvider);
    }

    private TasmoWriteMaterializer buildViewMaterializer(ViewsProvider viewsProvider,
        RowColumnValueStoreProvider rowColumnValueStoreProvider,
        WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider,
        CommitChange commitChange, TenantId masterTenantId) throws Exception {

        if (viewChangeNotificationProcessor == null) {
            viewChangeNotificationProcessor = new ViewChangeNotificationProcessor() {
                @Override
                public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
                }
            };
        }

        ConcurrencyStore concurrencyStore = buildConcurrencyStore(rowColumnValueStoreProvider);
        commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, commitChange);

        ReferenceStore referenceStore = buildReferenceStore(concurrencyStore, rowColumnValueStoreProvider);
        EventValueStore eventValueStore = buildEventValueStore(concurrencyStore, rowColumnValueStoreProvider);

        TasmoViewModel viewMaterializerModel = new TasmoViewModel(masterTenantId,
            viewsProvider,
            viewPathKeyProvider);

        viewMaterializerModel.loadModel(masterTenantId);

        WrittenEventProcessorDecorator writtenEventProcessorDecorator = new WrittenEventProcessorDecorator() {
            @Override
            public WrittenEventProcessor decorateWrittenEventProcessor(WrittenEventProcessor writtenEventProcessor) {
                return new EventBookKeeper(writtenEventProcessor);

            }
        };

        ListeningExecutorService traverserExecutors = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(32));
        final BatchingReferenceTraverser referenceTraverser = new BatchingReferenceTraverser(referenceStore,
            traverserExecutors, 100, 10_000); // TODO expose to config
        Executors.newSingleThreadExecutor().submit(new Runnable() {

            @Override
            public void run() {
                try {
                    referenceTraverser.startProcessingRequests();
                } catch (InterruptedException x) {
                    LOG.error("Reference Traversal failed for the folloing reasons.", x);
                    Thread.currentThread().interrupt();
                }
            }
        });

        TasmoEventTraversal eventTraverser = new TasmoEventTraverser(writtenEventProcessorDecorator,
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)));

        WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

        WriteFanoutEventPersistor eventPersistor = new WriteFanoutEventPersistor(writtenEventProvider,
            writtenInstanceHelper, concurrencyStore, eventValueStore, referenceStore);

        ProcessingStats processingStats = new ProcessingStats();
        StatCollectingFieldValueReader fieldValueReader = new StatCollectingFieldValueReader(processingStats,
            new EventValueStoreFieldValueReader(eventValueStore));

        TasmoEventProcessor tasmoEventProcessor = new TasmoEventProcessor(viewMaterializerModel,
            eventPersistor,
            writtenEventProvider,
            eventTraverser,
            viewChangeNotificationProcessor,
            new NoOpViewNotificationListener(),
            concurrencyStore,
            referenceStore,
            fieldValueReader,
            referenceTraverser,
            commitChange,
            processingStats);

        return new TasmoWriteMaterializer(new CallbackStream<List<BookkeepingEvent>>() {
            @Override
            public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                return value;
            }
        },
            tasmoEventProcessor,
            MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor()),
            new TasmoBlacklist());
    }

    private EventWriter buildEventWriter(final TasmoWriteMaterializer viewMaterializer,
        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider) {

        if (orderIdProvider == null) {
            orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        }
        final OrderIdProvider idProvider = orderIdProvider;
        JsonEventWriter jsonEventWriter = new JsonEventWriter() {
            @Override
            public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException {
                try {
                    JsonEventConventions jsonEventConventions = new JsonEventConventions();
                    List<ObjectId> objectIds = Lists.newArrayList();
                    List<Long> eventIds = Lists.newArrayList();
                    List<WrittenEvent> writtenEvents = new ArrayList<>();

                    for (ObjectNode w : events) {
                        long eventId = idProvider.nextId();
                        eventIds.add(eventId);
                        jsonEventConventions.setEventId(w, eventId);

                        String instanceClassname = jsonEventConventions.getInstanceClassName(w);
                        ObjectId objectId = new ObjectId(instanceClassname, jsonEventConventions.getInstanceId(w, instanceClassname));
                        objectIds.add(objectId);

                    }
                    for (ObjectNode eventNode : events) {
                        writtenEvents.add(writtenEventProvider.convertEvent(eventNode));
                    }
                    List<WrittenEvent> failedToProcess = viewMaterializer.process(writtenEvents);
                    while (!failedToProcess.isEmpty()) {
                        System.out.println("FAILED to process " + failedToProcess.size() + " events likely due to consistency issues.");
                        failedToProcess = viewMaterializer.process(failedToProcess);
                    }
                    return new EventWriterResponse(eventIds, objectIds);

                } catch (Exception ex) {
                    throw new JsonEventWriteException("sad trombone", ex);
                }
            }
        };

        return new EventWriter(jsonEventWriter);
    }

    private ViewReader<ViewResponse> buildViewReader(ViewValueStore viewValueStore, ViewsProvider viewsProvider, TenantId tenantId) {
        ViewValueReader viewValueReader = new ViewValueReader(viewValueStore);
        ViewPermissionChecker viewPermissionChecker = new ViewPermissionChecker() {
            @Override
            public ViewPermissionCheckResult check(TenantId tenantId, Id actorId, final Set<Id> permissionCheckTheseIds) {
                return new ViewPermissionCheckResult() {
                    @Override
                    public Set<Id> allowed() {
                        return permissionCheckTheseIds;
                    }

                    @Override
                    public Set<Id> denied() {
                        return Collections.emptySet();
                    }

                    @Override
                    public Set<Id> unknown() {
                        return Collections.emptySet();
                    }
                };
            }
        };

        ObjectMapper viewObjectMapper = new ObjectMapper();
        viewObjectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        JsonViewMerger merger = new JsonViewMerger(viewObjectMapper);
        ViewAsObjectNode viewAsObjectNode = new ViewAsObjectNode();

        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                //no op
            }
        };

        TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(tenantId, viewsProvider, viewPathKeyProvider);
        tenantViewsProvider.loadModel(tenantId);

        return new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            viewAsObjectNode,
            merger,
            staleViewFieldStream,
            1_024 * 1_024 * 10);
    }

    private String getViewClassFromViewModel(ObjectNode viewNode) {
        for (Iterator<String> it = viewNode.fieldNames(); it.hasNext();) {
            String fieldName = it.next();

            JsonNode got = viewNode.get(fieldName);
            if (got != null && !got.isNull() && got.isObject() && got.has(ReservedFields.VIEW_OBJECT_ID)) {
                return fieldName;
            }
        }

        return "";
    }

    private ViewsProvider buildViewsProvider(TenantId masterTenantId, List<ViewBinding> viewDefinitions) throws Exception {

        final ChainedVersion chainedVersion = new ChainedVersion("0", "1");

        final Views views = new Views(masterTenantId, chainedVersion, viewDefinitions);

        return new ViewsProvider() {
            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return chainedVersion;
            }

            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                return views;
            }
        };
    }

    private CommitChange buildCommitChange(ViewValueStore viewValueStore) throws Exception {

        ViewValueWriter viewValueWriter = new ViewValueWriter(viewValueStore);

        final WriteToViewValueStore writeToViewValueStore = new WriteToViewValueStore(viewValueWriter);
        return new CommitChange() {
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

                        write.add(new ViewWriteFieldChange(
                            change.getEventId(),
                            tenantIdAndCentricId,
                            change.getActorId(),
                            ViewWriteFieldChange.Type.valueOf(change.getType().name()),
                            change.getViewObjectId(),
                            change.getModelPathIdHashcode(),
                            ids,
                            new ViewValue(change.getModelPathTimestamps(), change.getValue()),
                            change.getTimestamp()));
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
    }

    @Override
    public void onShutDown() {
        try {
            rowColumnValueStoreProvider.shutdownUnderlyingStores();
        } catch (Exception ex) {
            LOG.error("Failure shutting down underlying materializer stores", ex);
        }
    }
}

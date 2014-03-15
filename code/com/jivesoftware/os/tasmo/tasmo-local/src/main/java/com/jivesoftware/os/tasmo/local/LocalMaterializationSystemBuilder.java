package com.jivesoftware.os.tasmo.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.configuration.BindingGenerator;
import com.jivesoftware.os.tasmo.configuration.ViewModel;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.TasmoViewMaterializer;
import com.jivesoftware.os.tasmo.lib.DispatcherProvider;
import com.jivesoftware.os.tasmo.lib.events.EventValueCacheProvider;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
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
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriteFieldChange;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriterException;
import com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public class LocalMaterializationSystemBuilder implements LocalMaterializationSystem.ShutdownCallback {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private Set<String> filterToTheseViewClasses;
    private RowColumnValueStoreProvider rowColumnValueStoreProvider;
    private ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private OrderIdProvider orderIdProvider;

    public LocalMaterializationSystemBuilder setViewChangeNotificationProcessor(ViewChangeNotificationProcessor viewChangeNotificationProcessor) {
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        return this;
    }

    public LocalMaterializationSystemBuilder setFilterToTheseViewClasses(Class... viewClasses) {
        this.filterToTheseViewClasses = Sets.newHashSet(Iterables.transform(Arrays.asList(viewClasses), new Function<Class, String>() {
            @Override
            public String apply(Class viewClass) {
                return viewClass.getSimpleName();
            }
        }));

        return this;
    }

    public LocalMaterializationSystemBuilder setOrderIdProvider(OrderIdProvider provider) {
        this.orderIdProvider = provider;
        return this;
    }

    public LocalMaterializationSystem build(Iterable<ObjectNode> viewDefinitions) throws Exception {
        TenantId masterTenantId = new TenantId("ephemeral");

        ViewsProvider viewsProvider = buildViewsProvider(masterTenantId, viewDefinitions);

        WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();

        RowColumnValueStoreUtil rowColumnValueStoreUtil = new RowColumnValueStoreUtil();

        String uuid = UUID.randomUUID().toString();
       
       rowColumnValueStoreProvider = rowColumnValueStoreUtil.getInMemoryRowColumnValueStoreProvider(uuid, writtenEventProvider);

        ViewValueStore viewValueStore = buildViewValueStore(rowColumnValueStoreProvider);
        CommitChange commitChange = buildCommitChange(viewValueStore);
        TasmoViewMaterializer viewMaterializer = buildViewMaterializer(viewsProvider, rowColumnValueStoreProvider,
            writtenEventProvider, commitChange, masterTenantId);

        EventWriter eventWriter = buildEventWriter(viewMaterializer, writtenEventProvider);
        ViewReader<ViewResponse> viewReader = buildViewReader(viewValueStore, viewsProvider, masterTenantId);

        return new LocalMaterializationSystem(eventWriter, viewReader, this);
    }

    private ReferenceStore buildReferenceStore(RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new ReferenceStore(rowColumnValueStoreProvider.multiLinks(), rowColumnValueStoreProvider.multiBackLinks());
    }

    private EventValueStore buildEventValueStore(RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new EventValueStore(rowColumnValueStoreProvider.eventStore(), new EventValueCacheProvider() {
            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> createValueStoreCache() {
                return new RowColumnValueStoreImpl<>();
            }
        });
    }

    private ExistenceStore buildExistenceStore(RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new ExistenceStore(rowColumnValueStoreProvider.existenceStore());
    }

    private ViewValueStore buildViewValueStore(RowColumnValueStoreProvider rowColumnValueStoreProvider) throws Exception {
        return new ViewValueStore(rowColumnValueStoreProvider.viewValueStore(), new ViewPathKeyProvider());
    }

    private TasmoViewMaterializer buildViewMaterializer(ViewsProvider viewsProvider,
        RowColumnValueStoreProvider rowColumnValueStoreProvider,
        WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider,
        CommitChange commitChange, TenantId masterTenantId) throws Exception {

        TasmoEventBookkeeper materializerEventBookkeeper = new TasmoEventBookkeeper(new CallbackStream<List<BookkeepingEvent>>() {
            @Override
            public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                return value;
            }
        });

        if (viewChangeNotificationProcessor == null) {
            viewChangeNotificationProcessor = new ViewChangeNotificationProcessor() {
                @Override
                public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
                }
            };
        }

        ExistenceStore existenceStore = buildExistenceStore(rowColumnValueStoreProvider);
        commitChange = new ExistenceCommitChange(existenceStore, commitChange);

        DispatcherProvider viewMaterializerModel = new DispatcherProvider(masterTenantId,
            viewsProvider,
            writtenEventProvider,
            buildReferenceStore(rowColumnValueStoreProvider),
            buildEventValueStore(rowColumnValueStoreProvider),
            commitChange);

        viewMaterializerModel.loadModel(masterTenantId);

        return new TasmoViewMaterializer(existenceStore, materializerEventBookkeeper, viewMaterializerModel, viewChangeNotificationProcessor);
    }

    private EventWriter buildEventWriter(final TasmoViewMaterializer viewMaterializer,
        final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider) {

        if (orderIdProvider == null) {
            orderIdProvider = new OrderIdProviderImpl(1);
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
                    viewMaterializer.process(writtenEvents);
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
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, String, Long> value) {
                //no op
            }
        };

        TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(tenantId, viewsProvider);
        tenantViewsProvider.loadModel(tenantId);

        return new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            viewAsObjectNode,
            merger,
            staleViewFieldStream);
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

    private ViewsProvider buildViewsProvider(TenantId masterTenantId, Iterable<ObjectNode> viewDefinitions) throws Exception {

        final ChainedVersion chainedVersion = new ChainedVersion("0", "1");
        BindingGenerator bindingGenerator = new BindingGenerator();
        List<ViewBinding> viewBindings = new LinkedList<>();

        for (ObjectNode v : viewDefinitions) {
            if (v != null) {
                if (filterToTheseViewClasses == null
                    || filterToTheseViewClasses.contains(getViewClassFromViewModel(v))) {

                    ViewModel viewConfiguration = ViewModel.builder(v).build();
                    viewBindings.add(bindingGenerator.generate(null, viewConfiguration));
                }
            }
        }

        final Views views = new Views(masterTenantId, chainedVersion, viewBindings);

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
        final ObjectMapper mapper = new ObjectMapper();

        ViewValueWriter viewValueWriter = new ViewValueWriter(viewValueStore);

        final WriteToViewValueStore writeToViewValueStore = new WriteToViewValueStore(viewValueWriter);
        return new CommitChange() {
            @Override
            public void commitChange(TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
                List<ViewWriteFieldChange> write = new ArrayList<>(changes.size());
                for (ViewFieldChange change : changes) {
                    try {
                        write.add(new ViewWriteFieldChange(
                            change.getEventId(),
                            -1,
                            -1,
                            tenantIdAndCentricId,
                            change.getActorId(),
                            ViewWriteFieldChange.Type.valueOf(change.getType().name()),
                            change.getViewObjectId(),
                            change.getModelPathId(),
                            change.getModelPathInstanceIds(),
                            mapper.writeValueAsString(change.getValue()),
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

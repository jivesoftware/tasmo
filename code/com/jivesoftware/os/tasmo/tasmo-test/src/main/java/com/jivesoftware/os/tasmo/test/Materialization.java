package com.jivesoftware.os.tasmo.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterOptions;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.JsonEventWriter;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.lib.TasmoBlacklist;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoWriteMaterializer;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.TasmoEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.TasmoEventTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.TasmoEventTraverser;
import com.jivesoftware.os.tasmo.lib.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.lib.read.StatCollectingFieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.lib.write.WriteFanoutEventPersistor;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
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
import com.jivesoftware.os.tasmo.reference.lib.traverser.BatchingReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.view.notification.api.NoOpViewNotificationListener;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Materialization {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public static final TenantId MASTER_TENANT_ID = new TenantId("master");
    EventValueStore eventValueStore;
    ViewValueStore viewValueStore;
    ViewValueWriter viewValueWriter;
    ViewValueReader viewValueReader;
    ViewProvider<ViewResponse> viewProvider;
    TasmoViewModel tasmoViewModel;
    TasmoWriteMaterializer tasmoMaterializer;
    final ChainedVersion currentVersion = new ChainedVersion("0", "1");
    final AtomicReference<Views> views = new AtomicReference<>();
    final ViewPathKeyProvider viewPathKeyProvider = new MurmurHashViewPathKeyProvider();
    ViewsProvider viewsProvider;
    boolean useHBase = false;
    ObjectMapper mapper = new ObjectMapper();

    {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    }
    WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider = new JsonWrittenEventProvider();

    public ViewChangeNotificationProcessor getViewChangeNotificationProcessor() {
        // default is a no op processor
        return new ViewChangeNotificationProcessor() {
            @Override
            public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
            }
        };
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
//                        writtenEventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));
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
            public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore() {
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

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() throws Exception;

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() throws Exception;
    }
    //private static EmbeddedHBase embeddedHBase = new EmbeddedHBase();

    public void startHBase() throws Exception {
        if (useHBase) {
            //embeddedHBase.start(true);
        }
    }

    public void stopHBase() throws Exception {
        if (useHBase) {
            //embeddedHBase.stop();
        }
    }

    RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> rawViewValueStore;
    private ExecutorService eventProcessorThreads;
    private ExecutorService pathProcessorThreads;
    private ExecutorService batchTraverserThread;
    private ExecutorService traverserThreads;
    private BatchingReferenceTraverser batchingReferenceTraverser;
    TasmoEventProcessor tasmoEventProcessor;
    TasmoProcessingStats processingStats;

    private ExecutorService newThreadPool(int maxThread, String name) {
        ThreadFactory eventProcessorThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat(name + "-%d")
            .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Thread " + t.getName() + " threw uncaught exception", e);
                }
            })
            .build();

        return new ThreadPoolExecutor(0, maxThread,
            5L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), eventProcessorThreadFactory);
    }

    public void setupModelAndMaterializer(int numberOfEventProcessorThreads) throws Exception {

        String uuid = UUID.randomUUID().toString();

        RowColumnValueStoreProvider rowColumnValueStoreProvider = getRowColumnValueStoreProvider(uuid);
        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore = rowColumnValueStoreProvider.eventStore();

        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrency = new RowColumnValueStoreImpl<>();
        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(concurrency);
        eventValueStore = new EventValueStore(concurrencyStore, eventStore);

        rawViewValueStore = rowColumnValueStoreProvider.viewValueStore();
        viewValueStore = new ViewValueStore(rawViewValueStore, viewPathKeyProvider);
        viewValueWriter = new ViewValueWriter(viewValueStore);
        viewValueReader = new ViewValueReader(viewValueStore);

        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, rowColumnValueStoreProvider.multiLinks(),
            rowColumnValueStoreProvider.multiBackLinks());

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

        commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, commitChange);


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

        pathProcessorThreads = newThreadPool(numberOfEventProcessorThreads, "process-path-");
        tasmoViewModel = new TasmoViewModel(
            MASTER_TENANT_ID,
            viewsProvider,
            viewPathKeyProvider);

        WrittenEventProcessorDecorator writtenEventProcessorDecorator = new WrittenEventProcessorDecorator() {
            @Override
            public WrittenEventProcessor decorateWrittenEventProcessor(WrittenEventProcessor writtenEventProcessor) {
                return new EventBookKeeper(writtenEventProcessor);
            }
        };

        traverserThreads = newThreadPool(numberOfEventProcessorThreads, "travers-path-");
        ListeningExecutorService traverserExecutor = MoreExecutors.listeningDecorator(traverserThreads);
        batchingReferenceTraverser = new BatchingReferenceTraverser(referenceStore, traverserExecutor, 100, 10_000); // TODO expose to config
        batchTraverserThread = newThreadPool(1, "batch-traverser");
        batchTraverserThread.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    batchingReferenceTraverser.startProcessingRequests();
                } catch (InterruptedException x) {
                    LOG.error("Reference Traversal failed for the folloing reasons.", x);
                    Thread.currentThread().interrupt();
                }
            }
        });

        ReferenceTraverser referenceTraverser = batchingReferenceTraverser;
//        ReferenceTraverser referenceTraverser = new SerialReferenceTraverser(referenceStore); //??

        TasmoEventTraversal eventTraverser = new TasmoEventTraverser(writtenEventProcessorDecorator,
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)));

        WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

        WriteFanoutEventPersistor eventPersistor = new WriteFanoutEventPersistor(writtenEventProvider,
            writtenInstanceHelper, concurrencyStore, eventValueStore, referenceStore);

        processingStats = new TasmoProcessingStats();
        StatCollectingFieldValueReader fieldValueReader = new StatCollectingFieldValueReader(processingStats,
            new EventValueStoreFieldValueReader(eventValueStore));

        tasmoEventProcessor = new TasmoEventProcessor(tasmoViewModel,
            eventPersistor,
            writtenEventProvider,
            eventTraverser,
            getViewChangeNotificationProcessor(),
            new NoOpViewNotificationListener(),
            concurrencyStore,
            referenceStore,
            fieldValueReader,
            referenceTraverser,
            commitChange,
            processingStats);

        eventProcessorThreads = newThreadPool(numberOfEventProcessorThreads, "process-event-");
        tasmoMaterializer = new TasmoWriteMaterializer(new CallbackStream<List<BookkeepingEvent>>() {
                @Override
                public List<BookkeepingEvent> callback(List<BookkeepingEvent> value) throws Exception {
                    return value;
                }
            },
            tasmoEventProcessor,
            MoreExecutors.listeningDecorator(eventProcessorThreads),
            new TasmoBlacklist());

    }

    public void logStats() {
        processingStats.logStats();
    }

    Expectations initModelPaths(TenantId tenantId, List<ViewBinding> bindings) throws Exception {
        Views newViews = new Views(tenantId, currentVersion, bindings);
        views.set(newViews);

        tasmoViewModel.loadModel(MASTER_TENANT_ID);

        JsonViewMerger merger = new JsonViewMerger(new ObjectMapper());
        ViewAsObjectNode viewAsObjectNode = new ViewAsObjectNode();

        ViewPermissionChecker viewPermissionChecker = new ViewPermissionChecker() {
            @Override
            public ViewPermissionCheckResult check(TenantId tenantId, Id actorId, final Set<Id> permissionCheckTheseIds) {
                //System.out.println("NO-OP permisions check for (" + permissionCheckTheseIds.size() + ") ids.");
                return new ViewPermissionCheckResult() {
                    @Override
                    public Set<Id> allowed() {
                        return permissionCheckTheseIds;
                    }

                    @Override
                    public Set<Id> denied() {
                        return new HashSet<>();
                    }

                    @Override
                    public Set<Id> unknown() {
                        return new HashSet<>();
                    }
                };
            }
        };
        TenantId masterTenantId = new TenantId("master");
        TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(masterTenantId, viewsProvider, viewPathKeyProvider);
        tenantViewsProvider.loadModel(masterTenantId);

        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                System.out.println("Encounterd stale fields for:" + viewDescriptor + " value:" + value);
            }
        };

        viewProvider = new ViewProvider<>(viewPermissionChecker,
            viewValueReader,
            tenantViewsProvider,
            viewAsObjectNode,
            merger,
            staleViewFieldStream,
            1_024 * 1_024 * 10);
        return new Expectations(viewValueStore, newViews, viewPathKeyProvider);

    }

    public void shutdown() {
        batchingReferenceTraverser.stopProcessingRequests();
        eventProcessorThreads.shutdownNow();
        pathProcessorThreads.shutdownNow();
        traverserThreads.shutdownNow();
        batchTraverserThread.shutdownNow();
    }

    static List<ViewBinding> parseModelPathStrings(boolean idCentric, List<String> simpleBindings) {
        ArrayListMultimap<String, ModelPath> viewBindings = ArrayListMultimap.create();

        for (String simpleBinding : simpleBindings) {
            String[] class_pathId_modelPath = toStringArray(simpleBinding, "::");
            List<ModelPath> bindings = viewBindings.get(class_pathId_modelPath[0].trim());

            bindings.add(buildPath(class_pathId_modelPath[1].trim(), class_pathId_modelPath[2].trim()));
        }

        List<ViewBinding> viewBindingsList = Lists.newArrayList();
        for (Map.Entry<String, Collection<ModelPath>> entry : viewBindings.asMap().entrySet()) {
            viewBindingsList.add(new ViewBinding(entry.getKey(), new ArrayList<>(entry.getValue()), false, idCentric, false, null));
        }

        return viewBindingsList;
    }

    Expectations initModelPaths(TenantId tenantId, boolean idCentric, List<String> simpleBindings) throws Exception {
        List<ViewBinding> viewBindingsList = parseModelPathStrings(idCentric, simpleBindings);
        return initModelPaths(tenantId, viewBindingsList);
    }

    static ModelPath buildPath(String id, String path) {
        String[] pathMembers = toStringArray(path, "|");
        ModelPath.Builder builder = ModelPath.builder(id);
        int i = 0;
        for (String pathMember : pathMembers) {
            builder.addPathMember(toModelPathMember(i, pathMember.trim()));
            i++;
        }
        return builder.build();
    }

    static ModelPathStep toModelPathMember(int sortPrecedence, String pathMember) {

        try {
            String[] memberParts = toStringArray(pathMember, ".");
            if (pathMember.contains("." + ModelPathStepType.ref + ".")
                    || pathMember.contains("." + ModelPathStepType.refs + ".")
                    || pathMember.contains("." + ModelPathStepType.centric_ref + ".")
                    || pathMember.contains("." + ModelPathStepType.centric_refs + ".")) {
                // Example: Content.ref_originalAuthor.ref.User
                Set<String> originClassName = splitClassNames(memberParts[0].trim());
                String refFieldName = memberParts[1].trim();
                ModelPathStepType stepType = ModelPathStepType.valueOf(memberParts[2].trim());
                Set<String> destinationClassName = splitClassNames(memberParts[3].trim());

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    refFieldName, stepType, destinationClassName, null);

            } else if (pathMember.contains("." + ModelPathStepType.backRefs + ".")
                || pathMember.contains("." + ModelPathStepType.count + ".")
                || pathMember.contains("." + ModelPathStepType.latest_backRef + ".")
                    || pathMember.contains("." + ModelPathStepType.centric_backRefs + ".")
                || pathMember.contains("." + ModelPathStepType.centric_count + ".")
                || pathMember.contains("." + ModelPathStepType.centric_latest_backRef + ".")) {

                // Example: Content.backRefs.VersionedContent.ref_parent
                // Example: Content.count.VersionedContent.ref_parent
                // Example: Content.latest_backRef.VersionedContent.ref_parent
                Set<String> destinationClassName = splitClassNames(memberParts[0].trim());
                ModelPathStepType stepType = ModelPathStepType.valueOf(memberParts[1].trim());
                Set<String> originClassName = splitClassNames(memberParts[2].trim());
                String refFieldName = memberParts[3].trim();

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    refFieldName, stepType, destinationClassName, null);

            } else {

                // Example: User.firstName
                String[] valueFieldNames = toStringArray(memberParts[1], ",");
                for (int i = 0; i < valueFieldNames.length; i++) {
                    valueFieldNames[i] = valueFieldNames[i].trim();
                }
                Set<String> originClassName = splitClassNames(memberParts[0].trim());

                return new ModelPathStep(sortPrecedence == 0, originClassName,
                    null, ModelPathStepType.value, null, Arrays.asList(valueFieldNames));

            }
        } catch (Exception x) {
            throw new RuntimeException("fail to parse " + pathMember, x);
        }
    }

    static private Set<String> splitClassNames(String classNames) {
        if (classNames.startsWith("[")) {
            classNames = classNames.replace("[", "");
            classNames = classNames.replace("]", "");

            return Sets.newHashSet(classNames.split("\\^"));
        } else {
            return Sets.newHashSet(classNames);
        }
    }

    JsonEventWriter jsonEventWriter(final Materialization materialization, final OrderIdProvider idProvider) {
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

                    List<WrittenEvent> failedToProcess = materialization.tasmoMaterializer.process(writtenEvents);
                    while (!failedToProcess.isEmpty()) {
                        System.out.println("FAILED to process " + failedToProcess.size() + " events likely due to consistency issues.");
                        failedToProcess = materialization.tasmoMaterializer.process(failedToProcess);
                    }
                    return new EventWriterResponse(eventIds, objectIds);

                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new JsonEventWriteException("sad trombone", ex);
                }
            }
        };
    }

    protected ObjectNode readView(TenantIdAndCentricId tenantIdAndCentricId, Id actorId, ObjectId viewId) throws IOException {
        ViewResponse viewResponse = viewProvider.readView(new ViewDescriptor(tenantIdAndCentricId, actorId, viewId));
        // kinda patch to refrain from refactoring dozens of tests... readView used to return null in case of a non-existing view
        return viewResponse.getStatusCode() == ViewResponse.StatusCode.OK ? viewResponse.getViewBody() : null;
    }

    static private String[] toStringArray(String string, String delim) {
        if (string == null || delim == null) {
            return new String[0];
        }
        StringTokenizer tokenizer = new StringTokenizer(string, delim);
        int tokenCount = tokenizer.countTokens();

        String[] tokens = new String[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = tokenizer.nextToken();
        }
        return tokens;
    }
}

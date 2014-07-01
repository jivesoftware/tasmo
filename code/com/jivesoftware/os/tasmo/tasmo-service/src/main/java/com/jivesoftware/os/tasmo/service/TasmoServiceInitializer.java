package com.jivesoftware.os.tasmo.service;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.lib.StatCollectingFieldValueReader;
import com.jivesoftware.os.tasmo.lib.TasmoEventProcessor;
import com.jivesoftware.os.tasmo.lib.TasmoEventTraverser;
import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.TasmoRetryingEventTraverser;
import com.jivesoftware.os.tasmo.lib.TasmoStorageProvider;
import com.jivesoftware.os.tasmo.lib.TasmoViewMaterializer;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.TasmoWriteFanoutEventPersistor;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.BatchingReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * TODO rename to TasmoWriteMaterializationInitializer
 */
public class TasmoServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoServiceConfig extends Config {

        @IntDefault(-1)
        public Integer getSessionIdCreatorId();

        @StringDefault("master")
        public String getModelMasterTenantId();

        @IntDefault(10)
        public Integer getPollForModelChangesEveryNSeconds();

        @IntDefault(1)
        public Integer getNumberOfEventProcessorThreads();
    }

    public static CallbackStream<List<WrittenEvent>> initializeEventIngressCallbackStream(
            OrderIdProvider threadTimestamp,
            ViewsProvider viewsProvider,
            ViewPathKeyProvider viewPathKeyProvider,
            WrittenEventProvider writtenEventProvider,
            TasmoStorageProvider tasmoStorageProvider,
            CommitChange changeWriter,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            CallbackStream<List<BookkeepingEvent>> bookKeepingStream,
            final Optional<WrittenEventProcessorDecorator> writtenEventProcessorDecorator,
            TasmoServiceConfig config) throws Exception {


        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

        TasmoEventBookkeeper bookkeeper = new TasmoEventBookkeeper(bookKeepingStream);
        TenantId masterTenantId = new TenantId(config.getModelMasterTenantId());

        final TasmoViewModel tasmoViewModel = new TasmoViewModel(
                masterTenantId,
                viewsProvider,
                viewPathKeyProvider,
                referenceStore);

        tasmoViewModel.loadModel(masterTenantId);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    tasmoViewModel.reloadModels();
                } catch (Exception x) {
                    LOG.error("Scheduled reloading of tasmo view model failed. ", x);
                }
            }
        }, config.getPollForModelChangesEveryNSeconds(), config.getPollForModelChangesEveryNSeconds(), TimeUnit.SECONDS);

        WrittenEventProcessorDecorator bookKeepingEventProcessor = new WrittenEventProcessorDecorator() {
            @Override
            public WrittenEventProcessor decorateWrittenEventProcessor(WrittenEventProcessor writtenEventProcessor) {
                EventBookKeeper eventBookKeeper = new EventBookKeeper(writtenEventProcessor);
                if (writtenEventProcessorDecorator.isPresent()) {
                    return writtenEventProcessorDecorator.get().decorateWrittenEventProcessor(eventBookKeeper);
                } else {
                    return eventBookKeeper;
                }

            }
        };

        ListeningExecutorService traverserExecutors = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool()); // TODO expose to config
        final BatchingReferenceTraverser batchingReferenceTraverser = new BatchingReferenceTraverser(referenceStore,
                traverserExecutors, 100, 10000); // TODO expose to config
        Executors.newSingleThreadExecutor().submit(new Runnable() {

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

        ReferenceTraverser referenceTraverser = batchingReferenceTraverser; //new SerialReferenceTraverser(referenceStore);

        TasmoEventTraverser eventTraverser = new TasmoRetryingEventTraverser(bookKeepingEventProcessor,
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)));

        WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

        TasmoWriteFanoutEventPersistor eventPersistor = new TasmoWriteFanoutEventPersistor(writtenEventProvider,
            writtenInstanceHelper, concurrencyStore, eventValueStore, referenceStore);

        final TasmoProcessingStats processingStats = new TasmoProcessingStats();
        StatCollectingFieldValueReader fieldValueReader = new StatCollectingFieldValueReader(processingStats,
            new EventValueStoreFieldValueReader(eventValueStore));

        TasmoEventProcessor tasmoEventProcessor = new TasmoEventProcessor(tasmoViewModel,
            eventPersistor,
            writtenEventProvider,
            eventTraverser,
            viewChangeNotificationProcessor,
            concurrencyStore,
            referenceStore,
            fieldValueReader,
            referenceTraverser,
            changeWriter,
            processingStats);


        ThreadFactory eventProcessorThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("event-processor-%d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LOG.error("Thread " + t.getName() + " threw uncaught exception", e);
                    }
                })
                .build();

        ExecutorService eventProcessorThreads = Executors.newFixedThreadPool(config.getNumberOfEventProcessorThreads(), eventProcessorThreadFactory);
        TasmoViewMaterializer materializer = new TasmoViewMaterializer(bookkeeper,
                tasmoEventProcessor,
                MoreExecutors.listeningDecorator(eventProcessorThreads));

        EventIngressCallbackStream eventIngressCallbackStream = new EventIngressCallbackStream(materializer);
        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .withWaitStrategy(WaitStrategies.randomWait(1, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.neverStop())
                .build();

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    processingStats.logStats();
                } catch (Exception x) {
                    LOG.error("Issue with logging stats. ", x);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);

        return new EventIngressRetryingCallbackStream(eventIngressCallbackStream, retryer);
    }
}

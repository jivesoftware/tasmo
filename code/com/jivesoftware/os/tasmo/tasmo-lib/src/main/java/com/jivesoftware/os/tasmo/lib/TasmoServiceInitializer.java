package com.jivesoftware.os.tasmo.lib;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.TasmoEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.TasmoProcessingStats;
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
import com.jivesoftware.os.tasmo.lib.write.TasmoWriteFanoutEventPersistor;
import com.jivesoftware.os.tasmo.lib.write.TasmoWriteMaterializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.SerialReferenceTraverser;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 * TODO rename to TasmoWriteMaterializationInitializer
 */
public class TasmoServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoServiceConfig extends Config {

        @IntDefault(-1)
        public Integer getSessionIdCreatorId();
        public void setSessionIdCreatorId(int idCreator);

        @IntDefault(1)
        public Integer getNumberOfEventProcessorThreads();
        public void setNumberOfEventProcessorThreads(int numberOfThreads);
    }

    public static TasmoEventIngress initialize(
            OrderIdProvider threadTimestamp,
            TasmoViewModel tasmoViewModel,
            WrittenEventProvider writtenEventProvider,
            TasmoStorageProvider tasmoStorageProvider,
            CommitChange commitChange,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
            final Optional<WrittenEventProcessorDecorator> writtenEventProcessorDecorator,
            TasmoBlacklist tasmoBlacklist,
            TasmoServiceConfig config) throws Exception {


        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

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

        ReferenceTraverser referenceTraverser = new SerialReferenceTraverser(referenceStore);

        TasmoEventTraversal eventTraverser = new TasmoEventTraverser(bookKeepingEventProcessor,
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1)));

        WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

        TasmoWriteFanoutEventPersistor eventPersistor = new TasmoWriteFanoutEventPersistor(writtenEventProvider,
            writtenInstanceHelper, concurrencyStore, eventValueStore, referenceStore);

        final TasmoProcessingStats processingStats = new TasmoProcessingStats();
        StatCollectingFieldValueReader fieldValueReader = new StatCollectingFieldValueReader(processingStats,
            new EventValueStoreFieldValueReader(eventValueStore));

        commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, commitChange);

        TasmoEventProcessor tasmoEventProcessor = new TasmoEventProcessor(tasmoViewModel,
            eventPersistor,
            writtenEventProvider,
            eventTraverser,
            viewChangeNotificationProcessor,
            concurrencyStore,
            referenceStore,
            fieldValueReader,
            referenceTraverser,
            commitChange,
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
        TasmoWriteMaterializer materializer = new TasmoWriteMaterializer(bookkeepingStream,
                tasmoEventProcessor,
                MoreExecutors.listeningDecorator(eventProcessorThreads), tasmoBlacklist);

        TasmoEventIngress tasmoEventIngress = new TasmoEventIngress(materializer);

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

        return tasmoEventIngress;
    }
}

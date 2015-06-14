package com.jivesoftware.os.tasmo.lib;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoEventIngress;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoNotificationsIngress;
import com.jivesoftware.os.tasmo.lib.ingress.TasmoWriteMaterializer;
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
import com.jivesoftware.os.tasmo.lib.write.NoOpCommitChange;
import com.jivesoftware.os.tasmo.lib.write.WriteFanoutEventPersistor;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.SerialReferenceTraverser;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotificationListener;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 * TODO rename to TasmoWriteMaterializationInitializer
 */
public class TasmoNotifierInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoNotifierConfig extends Config {

        @IntDefault (1)
        public Integer getNumberOfEventProcessorThreads();

        public void setNumberOfEventProcessorThreads(int numberOfThreads);
    }

    public static TasmoServiceHandle<TasmoEventIngress> initialize(
        TasmoViewModel tasmoViewModel,
        WrittenEventProvider writtenEventProvider,
        TasmoStorageProvider tasmoStorageProvider,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor, // TODO deprecate
        TasmoProcessingStats tasmoProcessingStats,
        TasmoBlacklist tasmoBlacklist,
        final Optional<WrittenEventProcessorDecorator> writtenEventProcessorDecorator,
        final Optional<ViewNotificationListener> delegateAllViewNotificationsListener,
        final Optional<TasmoNotificationsIngress> tasmoNotificationsIngress,
        TasmoNotifierConfig config) throws Exception {

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

        WriteFanoutEventPersistor eventPersistor = new WriteFanoutEventPersistor(writtenEventProvider,
            writtenInstanceHelper, concurrencyStore, eventValueStore, referenceStore);

        StatCollectingFieldValueReader fieldValueReader = new StatCollectingFieldValueReader(tasmoProcessingStats,
            new EventValueStoreFieldValueReader(eventValueStore));

        CommitChange commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, new NoOpCommitChange());

        ViewNotificationListener allViewNotificationsListener = new ViewNotificationListener() {

            @Override
            public void handleNotifications(List<ViewNotification> viewNotifications) throws Exception {
                if (tasmoNotificationsIngress.isPresent()) {
                    List<ViewNotification> failedToProcess = tasmoNotificationsIngress.get().callback(viewNotifications);
                    while (!failedToProcess.isEmpty()) {
                        // TODO add dead letter?
                        LOG.error("Failed to process these view notifications:" + failedToProcess);
                        failedToProcess = tasmoNotificationsIngress.get().callback(failedToProcess);
                    }
                }
                if (delegateAllViewNotificationsListener.isPresent()) {
                    delegateAllViewNotificationsListener.get().handleNotifications(viewNotifications);
                }
            }

            @Override
            public void flush() {
                if (delegateAllViewNotificationsListener.isPresent()) {
                    delegateAllViewNotificationsListener.get().flush();
                }
            }
        };

        TasmoEventProcessor tasmoEventProcessor = new TasmoEventProcessor(tasmoViewModel,
            eventPersistor,
            writtenEventProvider,
            eventTraverser,
            viewChangeNotificationProcessor,
            allViewNotificationsListener,
            concurrencyStore,
            referenceStore,
            fieldValueReader,
            referenceTraverser,
            commitChange,
            tasmoProcessingStats);

        ThreadFactory eventProcessorThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("event-notofication-processor-%d")
            .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Thread " + t.getName() + " threw uncaught exception", e);
                }
            })
            .build();

        final ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(config.getNumberOfEventProcessorThreads(), eventProcessorThreadFactory));

        CallbackStream<List<BookkeepingEvent>> noOpBookkeeping = new CallbackStream<List<BookkeepingEvent>>() {

            @Override
            public List<BookkeepingEvent> callback(List<BookkeepingEvent> v) throws Exception {
                return v;
            }
        };

        TasmoWriteMaterializer materializer = new TasmoWriteMaterializer(noOpBookkeeping,
            tasmoEventProcessor,
            listeningExecutorService,
            tasmoBlacklist);

        final TasmoEventIngress tasmoEventIngress = new TasmoEventIngress(materializer);
        return new TasmoServiceHandle<TasmoEventIngress>() {

            @Override
            public TasmoEventIngress getService() {
                return tasmoEventIngress;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
                listeningExecutorService.shutdownNow();
            }
        };
    }
}

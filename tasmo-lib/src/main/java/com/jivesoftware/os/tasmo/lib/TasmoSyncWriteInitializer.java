package com.jivesoftware.os.tasmo.lib;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.modifier.ModifierStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.write.EventPersistor;
import com.jivesoftware.os.tasmo.lib.write.SyncEventWriter;
import com.jivesoftware.os.tasmo.lib.write.SyncWriteEventPersistor;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 *
 */
public class TasmoSyncWriteInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoSyncWriteConfig extends Config {

        @IntDefault (1)
        public Integer getNumberOfSyncEventWritorThreads();
        public void setNumberOfSyncEventWritorThreads(Integer threads);
    }

    public static SyncEventWriter initialize(TasmoViewModel tasmoViewModel,
        WrittenEventProvider writtenEventProvider,
        TasmoStorageProvider tasmoStorageProvider,
        CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
        TasmoBlacklist tasmoBlacklist,
        TasmoSyncWriteConfig config) throws Exception {

        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

        EventPersistor eventPersistor = new SyncWriteEventPersistor(writtenEventProvider,
            new WrittenInstanceHelper(),
            concurrencyStore,
            eventValueStore,
            referenceStore);

        ThreadFactory syncEventWritorThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("sync-event-writer-%d")
            .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Thread " + t.getName() + " threw uncaught exception", e);
                }
            })
            .build();

        ExecutorService syncEventWritorThreads = Executors.newFixedThreadPool(config.getNumberOfSyncEventWritorThreads(), syncEventWritorThreadFactory);

        ModifierStore modifierStore = new ModifierStore(tasmoStorageProvider.modifierStorage());
        return new SyncEventWriter(MoreExecutors.listeningDecorator(syncEventWritorThreads),
            tasmoViewModel,
            eventPersistor,
            modifierStore,
            bookkeepingStream,
            tasmoBlacklist);
    }
}

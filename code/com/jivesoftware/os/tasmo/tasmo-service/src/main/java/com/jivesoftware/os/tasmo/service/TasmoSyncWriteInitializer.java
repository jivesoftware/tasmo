package com.jivesoftware.os.tasmo.service;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.TasmoBlacklist;
import com.jivesoftware.os.tasmo.lib.TasmoStorageProvider;
import com.jivesoftware.os.tasmo.lib.TasmoSyncEventWriter;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.write.TasmoEventPersistor;
import com.jivesoftware.os.tasmo.lib.write.TasmoSyncWriteEventPersistor;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 *
 */
public class TasmoSyncWriteInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoSyncWriteConfig extends Config {

        @StringDefault ("master")
        public String getModelMasterTenantId();

        @IntDefault (10)
        public Integer getPollForModelChangesEveryNSeconds();

        @IntDefault (1)
        public Integer getNumberOfSyncEventWritorThreads();
    }

    public static TasmoSyncEventWriter initializeEventIngressCallbackStream(
        ViewsProvider viewsProvider,
        ViewPathKeyProvider viewPathKeyProvider,
        WrittenEventProvider writtenEventProvider,
        TasmoStorageProvider tasmoStorageProvider,
        TasmoBlacklist tasmoBlacklist,
        TasmoSyncWriteConfig config) throws Exception {

        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

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

        TasmoEventPersistor eventPersistor = new TasmoSyncWriteEventPersistor(writtenEventProvider,
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

        return new TasmoSyncEventWriter(MoreExecutors.listeningDecorator(syncEventWritorThreads), tasmoViewModel, eventPersistor, tasmoBlacklist);
    }
}

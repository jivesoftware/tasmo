package com.jivesoftware.os.tasmo.lib;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.lib.read.FieldValueReader;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializerViewFields;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.SerialReferenceTraverser;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 *
 */
public class TasmoReadMaterializerInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface TasmoReadMaterializerConfig extends Config {

        @IntDefault (1)
        public Integer getNumberOfViewRequestProcessorThreads();

        public void setNumberOfViewRequestProcessorThreads(int numberOfThreads);
    }

    public static TasmoServiceHandle<ReadMaterializerViewFields> initialize(TasmoReadMaterializerConfig config,
        TasmoViewModel tasmoViewModel,
        WrittenEventProvider writtenEventProvider,
        TasmoStorageProvider tasmoStorageProvider) throws Exception {

        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore,
            tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

        // TODO add config option to switch between batching and serial.
        ReferenceTraverser referenceTraverser = new SerialReferenceTraverser(referenceStore);
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        FieldValueReader fieldValueReader = new EventValueStoreFieldValueReader(eventValueStore);

        ThreadFactory eventProcessorThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("view-read-materialization-processor-%d")
            .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Thread " + t.getName() + " threw uncaught exception", e);
                }
            })
            .build();

        ExecutorService processorThreads = Executors.newFixedThreadPool(config.getNumberOfViewRequestProcessorThreads(), eventProcessorThreadFactory);
        final ListeningExecutorService listeningDecorator = MoreExecutors.listeningDecorator(processorThreads);

        final ReadMaterializerViewFields readMaterializer = new ReadMaterializerViewFields(referenceTraverser,
            fieldValueReader, concurrencyStore, tasmoViewModel, listeningDecorator);

        return new TasmoServiceHandle<ReadMaterializerViewFields>() {

            @Override
            public ReadMaterializerViewFields getService() {
                return readMaterializer;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
                listeningDecorator.shutdown();
            }
        };
    }

}

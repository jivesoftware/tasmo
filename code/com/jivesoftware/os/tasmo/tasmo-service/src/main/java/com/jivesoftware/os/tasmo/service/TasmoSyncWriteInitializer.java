package com.jivesoftware.os.tasmo.service;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.write.TasmoEventPersistor;
import com.jivesoftware.os.tasmo.lib.TasmoStorageProvider;
import com.jivesoftware.os.tasmo.lib.write.TasmoSyncWriteEventPersistor;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import org.merlin.config.Config;

/**
 *
 *
 */
public class TasmoSyncWriteInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoSyncWriteConfig extends Config {
    }

    public static TasmoEventPersistor initializeEventIngressCallbackStream(
            WrittenEventProvider writtenEventProvider,
            TasmoStorageProvider tasmoStorageProvider,
            TasmoSyncWriteConfig config) throws Exception {


        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore, tasmoStorageProvider.multiLinksStorage(),
            tasmoStorageProvider.multiBackLinksStorage());

       TasmoEventPersistor eventPersistor = new TasmoSyncWriteEventPersistor(writtenEventProvider,
           new WrittenInstanceHelper(),
           concurrencyStore,
           eventValueStore,
           referenceStore);

        return eventPersistor;
    }
}

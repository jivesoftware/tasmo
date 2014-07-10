package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.lib.read.FieldValueReader;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.SerialReferenceTraverser;
import org.merlin.config.Config;

/**
 *
 *
 */
public class TasmoReadMaterializationInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface TasmoReadMaterializationConfig extends Config {
    }

    public static TasmoServiceHandle<ReadMaterializer> initialize(TasmoReadMaterializationConfig config,
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

        final ReadMaterializer readMaterializer = new ReadMaterializer(referenceTraverser, fieldValueReader, concurrencyStore, tasmoViewModel);

        return new TasmoServiceHandle<ReadMaterializer>() {

            @Override
            public ReadMaterializer getService() {
                return readMaterializer;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
            }
        };
    }

}

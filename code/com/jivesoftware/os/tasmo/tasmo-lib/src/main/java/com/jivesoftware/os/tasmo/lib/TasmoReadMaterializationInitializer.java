package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializedViewProvider;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.HBaseBackedConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.reference.lib.traverser.SerialReferenceTraverser;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReadMaterializer;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.ViewAsObjectNode;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 *
 */
public class TasmoReadMaterializationInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();


    public static ViewReadMaterializer<ViewResponse> initialize(TasmoReadMaterializationConfig config,
        ViewsProvider viewsProvider,
        ViewPathKeyProvider viewPathKeyProvider,
        WrittenEventProvider writtenEventProvider,
        TasmoStorageProvider tasmoStorageProvider,
        ViewPermissionChecker viewPermissionChecker,
        Optional<CommitChange> commitChangeVistor) throws Exception {


        ConcurrencyStore concurrencyStore = new HBaseBackedConcurrencyStore(tasmoStorageProvider.concurrencyStorage());
        ReferenceStore referenceStore = new ReferenceStore(concurrencyStore,
            tasmoStorageProvider.multiLinksStorage(),
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

        // TODO add config option to switch between batching and serial.
        ReferenceTraverser referenceTraverser = new SerialReferenceTraverser(referenceStore);
        EventValueStore eventValueStore = new EventValueStore(concurrencyStore, tasmoStorageProvider.eventStorage());
        FieldValueReader fieldValueReader = new EventValueStoreFieldValueReader(eventValueStore);
        ViewAsObjectNode viewAsObjectNode = new ViewAsObjectNode();

        ViewReadMaterializer<ViewResponse> viewReadMaterializer = new ReadMaterializedViewProvider<>(viewPermissionChecker,
            referenceTraverser,
            fieldValueReader,
            concurrencyStore,
            tasmoViewModel,
            viewAsObjectNode,
            new JsonViewMerger(new ObjectMapper()),
            commitChangeVistor,
            1_024 * 1_024 * 10); // TODO expose to config

        return viewReadMaterializer;
    }

    public static interface TasmoReadMaterializationConfig extends Config {

        @StringDefault ("master")
        public String getModelMasterTenantId();

        public void setModelMasterTenantId(String tenantId);

        @IntDefault (10)
        public Integer getPollForModelChangesEveryNSeconds();

        public void setPollForModelChangesEveryNSeconds(Integer seconds);
    }

}

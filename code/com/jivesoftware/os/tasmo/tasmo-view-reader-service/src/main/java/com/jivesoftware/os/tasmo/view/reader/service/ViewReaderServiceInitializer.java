package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.NeverAcceptsFailureSetOfSortedMaps;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.ByteArrayTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.StringTypeMarshaller;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.ObjectIdMarshaller;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.id.TenantIdMarshaller;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKeyMarshaller;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.lib.BatchingEventValueStore;
import com.jivesoftware.os.tasmo.view.reader.lib.BatchingReferenceStore;
import com.jivesoftware.os.tasmo.view.reader.lib.ExistenceChecker;
import com.jivesoftware.os.tasmo.view.reader.lib.JsonViewFormatterProvider;
import com.jivesoftware.os.tasmo.view.reader.lib.ReadTimeViewMaterializer;
import com.jivesoftware.os.tasmo.view.reader.lib.ReferenceGatherer;
import com.jivesoftware.os.tasmo.view.reader.lib.ValueGatherer;
import com.jivesoftware.os.tasmo.view.reader.lib.ViewModelProvider;
import com.jivesoftware.os.tasmo.view.reader.lib.ViewPermissionChecker;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 *
 */
public class ViewReaderServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface ViewReaderServiceConfig extends Config {

        @StringDefault("dev")
        public String getTableNameSpace();

        @StringDefault("master")
        public String getModelMasterTenantId();

        @IntDefault(10)
        public Integer getPollForModelChangesEveryNSeconds();

        @LongDefault(1000L * 60 * 60 * 24 * 30) // 30 days
        public Long getRemoveUndeclaredFieldsAfterNMillis();
    }

    public static ViewReader<ViewResponse> initializeViewReader(ViewReaderServiceConfig config,
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        ViewPermissionChecker viewPermissionChecker,
        ViewsProvider viewsProvider) throws Exception {

        return build(config, setOfSortedMapsImplInitializer, viewPermissionChecker, viewsProvider);
    }

    private static ViewReader<ViewResponse> build(ViewReaderServiceConfig config,
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        ViewPermissionChecker viewPermissionChecker,
        ViewsProvider viewsProvider) throws IOException {

        TenantId tenantId = new TenantId(config.getModelMasterTenantId());
        final ViewModelProvider viewModelProvider = new ViewModelProvider(tenantId, viewsProvider);
        viewModelProvider.loadModel(tenantId);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    viewModelProvider.reloadModels();
                } catch (Exception x) {
                    LOG.error("Scheduled reloading of view model failed. ", x);
                }
            }
        }, config.getPollForModelChangesEveryNSeconds(), config.getPollForModelChangesEveryNSeconds(), TimeUnit.SECONDS);

        WrittenEventProvider eventProvider = new JsonWrittenEventProvider();

        RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceTable =
            new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "existenceTable", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdMarshaller(),
            new ObjectIdMarshaller(), new StringTypeMarshaller(),
            new StringTypeMarshaller()), new CurrentTimestamper()));

        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore =
            new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "eventValueTable", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
            new ObjectIdMarshaller(), new StringTypeMarshaller(),
            (TypeMarshaller<OpaqueFieldValue>) eventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));


        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks =
            new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "multiLinkTable", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
            new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks =
            new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "multiBackLinkTable", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
            new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));


        BatchingReferenceStore batchingReferenceStore = new BatchingReferenceStore(multiLinks, multiBackLinks);
        ReferenceGatherer referenceGatherer = new ReferenceGatherer(batchingReferenceStore);

        BatchingEventValueStore batchingEventValueStore = new BatchingEventValueStore(eventStore);
        ValueGatherer valueGatherer = new ValueGatherer(batchingEventValueStore);
        JsonViewFormatterProvider jsonViewFormatterProvider = new JsonViewFormatterProvider(new ObjectMapper(), null);
        final ExistenceStore existenceStore = new ExistenceStore(existenceTable);

        ExistenceChecker existenceChecker = new ExistenceChecker() {
            @Override
            public Set<ObjectId> check(TenantId tenantId, Set<ObjectId> existenceCheckTheseIds) {
                return existenceStore.getExistence(tenantId, existenceCheckTheseIds);
            }
        };

        return new ReadTimeViewMaterializer(viewModelProvider, referenceGatherer, valueGatherer,
            jsonViewFormatterProvider, viewPermissionChecker, existenceChecker);
    }
}

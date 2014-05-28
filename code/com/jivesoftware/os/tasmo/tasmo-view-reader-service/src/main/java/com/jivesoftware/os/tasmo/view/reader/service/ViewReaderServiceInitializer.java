package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.NeverAcceptsFailureSetOfSortedMaps;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueMarshaller;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.IOException;
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

        @LongDefault(1024L * 1024L * 50) // 50 mb
        public Long getViewMaxSizeInBytes();
    }

    public static ViewReader<ViewResponse> initializeViewReader(ViewReaderServiceConfig config,
            SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
            ViewPermissionChecker viewPermissionChecker,
            ViewsProvider viewsProvider,
            ViewPathKeyProvider viewPathKeyProvider) throws Exception {
        return build(setOfSortedMapsImplInitializer,
                viewPermissionChecker,
                viewsProvider,
                viewPathKeyProvider,
                new ViewAsObjectNode(),
                config);
    }

    private static <V> ViewProvider<V> build(
            SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
            final ViewPermissionChecker viewPermissionChecker,
            ViewsProvider viewsProvider,
            ViewPathKeyProvider viewPathKeyProvider,
            ViewFormatter<V> viewFormatter,
            ViewReaderServiceConfig config) throws IOException {

        RowColumnValueStore<TenantIdAndCentricId,
                ImmutableByteArray,
                ImmutableByteArray,
                ViewValue,
                RuntimeException> store = new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(),
                "tasmo.views", "v", new DefaultRowColumnValueStoreMarshaller<>(
                        new TenantIdAndCentricIdMarshaller(),
                        new ImmutableByteArrayMarshaller(),
                        new ImmutableByteArrayMarshaller(),
                        new ViewValueMarshaller()), new CurrentTimestamper()));

        final ViewValueStore viewValueStore = new ViewValueStore(store, viewPathKeyProvider);
        ViewValueReader viewValueReader = new ViewValueReader(viewValueStore);

        TenantId tenantId = new TenantId(config.getModelMasterTenantId());
        final TenantViewsProvider tenantViewsProvider = new TenantViewsProvider(tenantId, viewsProvider, viewPathKeyProvider);
        tenantViewsProvider.loadModel(tenantId);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    tenantViewsProvider.reloadModels();
                } catch (Exception x) {
                    LOG.error("Scheduled reloadig of view model failed. ", x);
                }
            }
        }, config.getPollForModelChangesEveryNSeconds(), config.getPollForModelChangesEveryNSeconds(), TimeUnit.SECONDS);

        final long removeUndeclaredFieldsAfterNMillis = config.getRemoveUndeclaredFieldsAfterNMillis();
        StaleViewFieldStream staleViewFieldStream = new StaleViewFieldStream() {
            @Override
            public void stream(ViewDescriptor viewDescriptor, ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> value) {
                if (value != null && value.getTimestamp() < System.currentTimeMillis() - removeUndeclaredFieldsAfterNMillis) {
                    try {
                        ImmutableByteArray rowKey = viewValueStore.rowKey(viewDescriptor.getViewId());
                        viewValueStore.remove(viewDescriptor.getTenantIdAndCentricId(), rowKey, value.getColumn(), value.getTimestamp() + 1);
                    } catch (IOException x) {
                        LOG.warn("Failed trying to cleanup stale fields for:{} ", viewDescriptor);
                    }
                }
            }
        };

        long viewMaxSizeInBytes = config.getViewMaxSizeInBytes();

        return new ViewProvider<>(viewPermissionChecker,
                viewValueReader,
                tenantViewsProvider,
                viewFormatter,
                new JsonViewMerger(new ObjectMapper()),
                staleViewFieldStream,
                viewMaxSizeInBytes);
    }
}

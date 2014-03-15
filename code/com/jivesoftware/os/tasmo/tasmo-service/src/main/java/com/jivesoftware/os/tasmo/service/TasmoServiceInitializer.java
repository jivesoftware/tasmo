package com.jivesoftware.os.tasmo.service;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
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
import com.jivesoftware.os.tasmo.configuration.events.TenantEventsProvider;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.ObjectIdMarshaller;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.lib.TasmoViewMaterializer;
import com.jivesoftware.os.tasmo.lib.DispatcherProvider;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKeyMarshaller;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 *
 */
public class TasmoServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoServiceConfig extends Config {

        @StringDefault("dev")
        public String getTableNameSpace();

        @IntDefault(-1)
        public Integer getSessionIdCreatorId();

        @StringDefault("master")
        public String getModelMasterTenantId();

        @IntDefault(10)
        public Integer getPollForModelChangesEveryNSeconds();
    }

    public static EventIngressCallbackStream initializeEventIngressCallbackStream(
            TenantEventsProvider eventsProvider,
            WrittenEventProvider eventProvider,
            SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
            CommitChange changeWriter,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            CallbackStream<List<BookkeepingEvent>> bookKeepingStream,
            TasmoServiceConfig config) throws IOException {

        RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore =
                new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "eventValueTable", "v",
                new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
                new ObjectIdMarshaller(), new StringTypeMarshaller(),
                (TypeMarshaller<OpaqueFieldValue>) eventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));


        EventValueStore eventValueStore = new EventValueStore(eventStore);
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks =
                new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "multiLinkTable", "v",
                new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));

        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks =
                new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "multiBackLinkTable", "v",
                new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));

        ReferenceStore referenceStore = new ReferenceStore(multiLinks, multiBackLinks);

        TasmoEventBookkeeper bookkeeper =
                new TasmoEventBookkeeper(bookKeepingStream);
        TenantId masterTenantId = new TenantId(config.getModelMasterTenantId());

        final DispatcherProvider tasmoViewModel = new DispatcherProvider(
                eventsProvider,
                referenceStore,
                eventValueStore);

        tasmoViewModel.loadModel(masterTenantId);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    tasmoViewModel.reloadModels();
                } catch (Exception x) {
                    LOG.error("Scheduled reloadig of tasmo view model failed. ", x);
                }
            }
        }, config.getPollForModelChangesEveryNSeconds(), config.getPollForModelChangesEveryNSeconds(), TimeUnit.SECONDS);

        TasmoViewMaterializer materializer = new TasmoViewMaterializer(bookkeeper, tasmoViewModel, viewChangeNotificationProcessor);

        return new EventIngressCallbackStream(materializer);
    }
}

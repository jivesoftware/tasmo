package com.jivesoftware.os.tasmo.service;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.NeverAcceptsFailureSetOfSortedMaps;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.ByteArrayTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.StringTypeMarshaller;
import com.jivesoftware.os.tasmo.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.tasmo.id.ObjectIdMarshaller;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.lib.TasmoStorageProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKeyMarshaller;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueMarshaller;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public class HBaseBackedTasmoStorageProviderInitializer {

    static public interface HBaseBackedTasmoStorageProviderConfig extends Config {

        @StringDefault ("dev")
        public String getTableNameSpace();

    }

    public static TasmoStorageProvider initializeStorageProvider(
        final HBaseBackedTasmoStorageProviderConfig config,
        final SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        final WrittenEventProvider writtenEventProvider) {

        return new TasmoStorageProvider() {

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStorage()
                throws Exception {
                return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "tasmo.event.values", "v",
                    new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
                        new ObjectIdMarshaller(), new StringTypeMarshaller(),
                        (TypeMarshaller<OpaqueFieldValue>) writtenEventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrencyStorage()
                throws Exception {
                return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(),
                    "tasmo.multi.version.concurrency", "v",
                    new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
                        new ObjectIdMarshaller(), new StringTypeMarshaller(),
                        new LongTypeMarshaller()), new CurrentTimestamper()));
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage()
                throws Exception {
                return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(),
                    "tasmo.views", "v", new DefaultRowColumnValueStoreMarshaller<>(
                        new TenantIdAndCentricIdMarshaller(),
                        new ImmutableByteArrayMarshaller(),
                        new ImmutableByteArrayMarshaller(),
                        new ViewValueMarshaller()), new CurrentTimestamper()));
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinksStorage()
                throws Exception {
                return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "tasmo.links", "v",
                    new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                        new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));

            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinksStorage()
                throws Exception {
                return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(), "tasmo.back.links", "v",
                    new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                        new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));
            }
        };
    }
}

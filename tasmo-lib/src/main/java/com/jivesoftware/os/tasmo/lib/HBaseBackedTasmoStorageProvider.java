package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdMarshaller;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.ObjectIdMarshaller;
import com.jivesoftware.os.jive.utils.id.SaltingImmutableByteArrayMarshaller;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.jive.utils.id.TenantIdMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.NeverAcceptsFailureSetOfSortedMaps;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.ByteArrayTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.StringTypeMarshaller;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.id.ViewValueMarshaller;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKeyMarshaller;

/**
 *
 * @author jonathan.colt
 */
public class HBaseBackedTasmoStorageProvider implements TasmoStorageProvider {

    private final String tableNameSpace;
    private final SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer;
    private final WrittenEventProvider writtenEventProvider;

    public HBaseBackedTasmoStorageProvider(String tableNameSpace, SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        WrittenEventProvider writtenEventProvider) {
        this.tableNameSpace = tableNameSpace;
        this.setOfSortedMapsImplInitializer = setOfSortedMapsImplInitializer;
        this.writtenEventProvider = writtenEventProvider;
    }

    @Override
    public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStorage()
        throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace, "tasmo.event.values", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
                new ObjectIdMarshaller(), new StringTypeMarshaller(),
                (TypeMarshaller<OpaqueFieldValue>) writtenEventProvider.getLiteralFieldValueMarshaller()), new CurrentTimestamper()));
    }

    @Override
    public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrencyStorage()
        throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace,
            "tasmo.multi.version.concurrency", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(),
                new ObjectIdMarshaller(), new StringTypeMarshaller(),
                new LongTypeMarshaller()), new CurrentTimestamper()));
    }

    @Override
    public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStorage()
        throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace,
            "tasmo.views", "v", new DefaultRowColumnValueStoreMarshaller<>(
                new TenantIdAndCentricIdMarshaller(),
                new SaltingImmutableByteArrayMarshaller(),
                new ImmutableByteArrayMarshaller(),
                new ViewValueMarshaller()), new CurrentTimestamper()));
    }

    @Override
    public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinksStorage()
        throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace, "tasmo.links", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));

    }

    @Override
    public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinksStorage()
        throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace, "tasmo.back.links", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdAndCentricIdMarshaller(), new ClassAndField_IdKeyMarshaller(),
                new ObjectIdMarshaller(), new ByteArrayTypeMarshaller()), new CurrentTimestamper()));
    }

    @Override
    public RowColumnValueStore<TenantId, Id, ObjectId, String, RuntimeException> modifierStorage() throws Exception {
        return new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(tableNameSpace, "tasmo.modifier", "v",
            new DefaultRowColumnValueStoreMarshaller<>(new TenantIdMarshaller(),
                new IdMarshaller(), new ObjectIdMarshaller(),
                new StringTypeMarshaller()), new CurrentTimestamper()));
    }
}

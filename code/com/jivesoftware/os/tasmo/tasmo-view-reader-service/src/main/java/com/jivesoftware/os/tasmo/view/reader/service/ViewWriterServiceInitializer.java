package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.NeverAcceptsFailureSetOfSortedMaps;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueMarshaller;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 *
 */
public class ViewWriterServiceInitializer {

    static public interface ViewWriterServiceConfig extends Config {

        @StringDefault("dev")
        public String getTableNameSpace();
    }

    public static ViewValueWriter initializeViewWriter(ViewWriterServiceConfig config,
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        ViewPathKeyProvider viewPathKeyProvider) throws Exception {

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> store =
            new NeverAcceptsFailureSetOfSortedMaps<>(setOfSortedMapsImplInitializer.initialize(config.getTableNameSpace(),
            "tasmo.views", "v", new DefaultRowColumnValueStoreMarshaller<>(
            new TenantIdAndCentricIdMarshaller(),
            new ImmutableByteArrayMarshaller(),
            new ImmutableByteArrayMarshaller(),
            new ViewValueMarshaller()), new CurrentTimestamper()));

        ViewValueWriter viewValueWriter = new ViewValueWriter(new ViewValueStore(store, viewPathKeyProvider));
        return viewValueWriter;
    }
}

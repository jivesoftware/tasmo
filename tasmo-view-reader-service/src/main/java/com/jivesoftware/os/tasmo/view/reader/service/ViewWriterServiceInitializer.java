package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.NeverAcceptsFailureRowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.id.ImmutableByteArrayMarshaller;
import com.jivesoftware.os.rcvs.marshall.id.SaltingImmutableByteArrayMarshaller;
import com.jivesoftware.os.rcvs.marshall.id.TenantIdAndCentricIdMarshaller;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.id.ViewValueMarshaller;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
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
        RowColumnValueStoreInitializer<Exception> rowColumnValueStoreInitializer,
        ViewPathKeyProvider viewPathKeyProvider) throws Exception {

        RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> store =
            new NeverAcceptsFailureRowColumnValueStore<>(rowColumnValueStoreInitializer.initialize(
                    config.getTableNameSpace(),
                    "tasmo.views",
                    "v",
                    new DefaultRowColumnValueStoreMarshaller<>(
                        new TenantIdAndCentricIdMarshaller(),
                        new SaltingImmutableByteArrayMarshaller(),
                        new ImmutableByteArrayMarshaller(),
                        new ViewValueMarshaller()
                    ), new CurrentTimestamper()));

        ViewValueWriter viewValueWriter = new ViewValueWriter(new ViewValueStore(store, viewPathKeyProvider));
        return viewValueWriter;
    }
}

package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
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
        final RowColumnValueStoreInitializer<Exception> rowColumnValueStoreInitializer,
        final WrittenEventProvider writtenEventProvider) {

        return new HBaseBackedTasmoStorageProvider(config.getTableNameSpace(), rowColumnValueStoreInitializer, writtenEventProvider);
    }

    public static TasmoStorageProvider initializeSyncStorageProvider(
        final HBaseBackedTasmoStorageProviderConfig config,
        final RowColumnValueStoreInitializer<Exception> rowColumnValueStoreInitializer,
        final WrittenEventProvider writtenEventProvider) {
        return new HBaseBackedTasmoStorageProvider(config.getTableNameSpace() + ".sync", rowColumnValueStoreInitializer, writtenEventProvider);
    }
}

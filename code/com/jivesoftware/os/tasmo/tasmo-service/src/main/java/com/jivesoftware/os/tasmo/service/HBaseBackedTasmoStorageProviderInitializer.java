package com.jivesoftware.os.tasmo.service;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.tasmo.lib.TasmoStorageProvider;
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
        final SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        final WrittenEventProvider writtenEventProvider) {

        return new HBaseBackedTasmoStorageProvider(config.getTableNameSpace(), setOfSortedMapsImplInitializer, writtenEventProvider);
    }

    public static TasmoStorageProvider initializeSyncStorageProvider(
        final HBaseBackedTasmoStorageProviderConfig config,
        final SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsImplInitializer,
        final WrittenEventProvider writtenEventProvider) {
        return new HBaseBackedTasmoStorageProvider(config.getTableNameSpace()+".sync", setOfSortedMapsImplInitializer, writtenEventProvider);
    }
}

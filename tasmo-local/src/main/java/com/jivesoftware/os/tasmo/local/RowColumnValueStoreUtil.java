package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;


/**
 *
 */
public class RowColumnValueStoreUtil {

    public RowColumnValueStoreProvider getInMemoryRowColumnValueStoreProvider(String env, WrittenEventProvider writtenEventProvider) {
        return new RowColumnValueStoreProvider() {

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> concurrencyStore() {
                return new InMemoryRowColumnValueStore<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() {
                return new InMemoryRowColumnValueStore<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore() {
                return new InMemoryRowColumnValueStore<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() {
                return new InMemoryRowColumnValueStore<>();
            }

            @Override
            public RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() {
                return new InMemoryRowColumnValueStore<>();
            }

            @Override
            public void shutdownUnderlyingStores() throws Exception {
            }


        };
    }

}
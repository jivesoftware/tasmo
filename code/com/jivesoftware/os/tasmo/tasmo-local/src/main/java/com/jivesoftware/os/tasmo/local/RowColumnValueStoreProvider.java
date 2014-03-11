package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.tasmo.id.ImmutableByteArray;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;



/**
 *
 */
public interface RowColumnValueStoreProvider {

    RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore() throws Exception;

    RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventStore() throws Exception;

    RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, String, RuntimeException> viewValueStore() throws Exception;

    RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks() throws Exception;

    RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks() throws Exception;

    public void shutdownUnderlyingStores() throws Exception;
}
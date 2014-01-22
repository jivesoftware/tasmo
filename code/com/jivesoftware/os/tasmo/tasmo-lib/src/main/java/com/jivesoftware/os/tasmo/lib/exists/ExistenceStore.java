/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.exists;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ExistenceStore {

    private static final String existenceColumnKey = "existence"; // Uck
    private final RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> eventValueStore;

    public ExistenceStore(RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> classFieldValueStore) {
        this.eventValueStore = classFieldValueStore;
    }

    public void addObjectId(List<ExistanceUpdate> existanceUpdates) {

        List<TenantRowColumValueTimestampAdd<TenantId, ObjectId, String, String>> batch = new ArrayList<>();
        for (ExistanceUpdate existanceUpdate : existanceUpdates) {
            batch.add(new TenantRowColumValueTimestampAdd<>(existanceUpdate.tenantId,
                    existanceUpdate.objectId, existenceColumnKey,
                    Boolean.TRUE.toString(),
                    new ConstantTimestamper(existanceUpdate.timestamp)));
        }
        eventValueStore.multiRowsMultiAdd(batch);
    }

    public void removeObjectId(List<ExistanceUpdate> existanceUpdates) {
        List<TenantRowColumnTimestampRemove<TenantId, ObjectId, String>> batch = new ArrayList<>();
        for (ExistanceUpdate existanceUpdate : existanceUpdates) {
            batch.add(new TenantRowColumnTimestampRemove<>(existanceUpdate.tenantId,
                    existanceUpdate.objectId, existenceColumnKey,
                    new ConstantTimestamper(existanceUpdate.timestamp)));
        }
        eventValueStore.multiRowsMultiRemove(batch);
    }

    public Set<ObjectId> getExistence(List<ExistanceUpdate> existanceUpdates) {

        ListMultimap<TenantId, ObjectId> tenantIdsObjectIds = ArrayListMultimap.create();
        for (ExistanceUpdate existanceUpdate : existanceUpdates) {
            tenantIdsObjectIds.put(existanceUpdate.tenantId, existanceUpdate.objectId);
        }

        Set<ObjectId> existence = new HashSet<>();
        for (TenantId tenantId : tenantIdsObjectIds.keySet()) {
            List<ObjectId> orderObjectIds = tenantIdsObjectIds.get(tenantId);
            List<String> multiRowGet = eventValueStore.multiRowGet(tenantId, orderObjectIds, existenceColumnKey, null, null);
            for (int i = 0; i < orderObjectIds.size(); i++) {
                if (multiRowGet.get(i) != null) {
                    existence.add(orderObjectIds.get(i));
                }
            }
        }
        return existence;
    }

    public Set<ObjectId> getExistence(TenantId tenantId, Set<ObjectId> objectIds) {
        List<ObjectId> orderObjectIds = new ArrayList<>(objectIds);
        List<String> multiRowGet = eventValueStore.multiRowGet(tenantId, orderObjectIds, existenceColumnKey, null, null);
        Set<ObjectId> existence = new HashSet<>();
        for (int i = 0; i < orderObjectIds.size(); i++) {
            if (multiRowGet.get(i) != null) {
                existence.add(orderObjectIds.get(i));
            }
        }
        return existence;
    }

}

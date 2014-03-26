/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.existence;

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
    private final RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore;

    public ExistenceStore(RowColumnValueStore<TenantId, ObjectId, String, String, RuntimeException> existenceStore) {
        this.existenceStore = existenceStore;
    }

    public void addObjectId(List<ExistenceUpdate> existenceUpdates) {

        List<TenantRowColumValueTimestampAdd<TenantId, ObjectId, String, String>> batch = new ArrayList<>();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            batch.add(new TenantRowColumValueTimestampAdd<>(existenceUpdate.tenantId,
                    existenceUpdate.objectId, existenceColumnKey,
                    Boolean.TRUE.toString(),
                    new ConstantTimestamper(existenceUpdate.timestamp)));
        }
        existenceStore.multiRowsMultiAdd(batch);
    }

    public void removeObjectId(List<ExistenceUpdate> existenceUpdates) {
        List<TenantRowColumnTimestampRemove<TenantId, ObjectId, String>> batch = new ArrayList<>();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            batch.add(new TenantRowColumnTimestampRemove<>(existenceUpdate.tenantId,
                    existenceUpdate.objectId, existenceColumnKey,
                    new ConstantTimestamper(existenceUpdate.timestamp)));
        }
        existenceStore.multiRowsMultiRemove(batch);
    }

    public Set<ObjectId> getExistence(List<ExistenceUpdate> existenceUpdates) {

        ListMultimap<TenantId, ObjectId> tenantIdsObjectIds = ArrayListMultimap.create();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            tenantIdsObjectIds.put(existenceUpdate.tenantId, existenceUpdate.objectId);
        }

        Set<ObjectId> existence = new HashSet<>();
        for (TenantId tenantId : tenantIdsObjectIds.keySet()) {
            List<ObjectId> orderObjectIds = tenantIdsObjectIds.get(tenantId);
            List<String> multiRowGet = existenceStore.multiRowGet(tenantId, orderObjectIds, existenceColumnKey, null, null);
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
        List<String> multiRowGet = existenceStore.multiRowGet(tenantId, orderObjectIds, existenceColumnKey, null, null);
        Set<ObjectId> existence = new HashSet<>();
        for (int i = 0; i < orderObjectIds.size(); i++) {
            if (multiRowGet.get(i) != null) {
                existence.add(orderObjectIds.get(i));
            }
        }
        return existence;
    }

}

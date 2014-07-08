package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.List;
import java.util.Set;

/**
 * Used to support Multiversion concurrency control
 */
public interface ConcurrencyStore {

    void addObjectId(List<ExistenceUpdate> existenceUpdates);

    /**
     *
     * @param tenantId
     * @param expectedSet
     * @return expected instance if no instance was modified.
     */
    Set<FieldVersion> checkIfModified(TenantIdAndCentricId tenantId, Set<FieldVersion> expectedSet);

    Set<ObjectId> getExistence(TenantIdAndCentricId tenantId, Set<ObjectId> objectIds);

    long highest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String field, long defaultTimestamp);

    List<Long> highests(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields);

    void removeObjectId(List<ExistenceUpdate> existenceUpdates);

    void updated(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields, long timestamp);
}

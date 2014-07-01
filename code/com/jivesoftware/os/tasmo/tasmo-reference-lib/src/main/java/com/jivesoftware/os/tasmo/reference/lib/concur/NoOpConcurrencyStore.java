package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class NoOpConcurrencyStore implements ConcurrencyStore {

    @Override
    public void addObjectId(List<ExistenceUpdate> existenceUpdates) {
    }

    @Override
    public Set<FieldVersion> checkIfModified(TenantIdAndCentricId tenantId, Set<FieldVersion> expectedSet) {
        return expectedSet;
    }

    @Override
    public Set<ObjectId> getExistence(TenantIdAndCentricId tenantId, Set<ObjectId> objectIds) {
        return objectIds;
    }

    @Override
    public long highest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String field, long defaultTimestamp) {
        return defaultTimestamp;
    }

    @Override
    public List<Long> highests(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields) {
        return null; // TODO hmm how to solve this
    }

    @Override
    public void removeObjectId(List<ExistenceUpdate> existenceUpdates) {
    }

    @Override
    public void updated(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields, long timestamp) {
    }

}

package com.jivesoftware.os.tasmo.lib.concur;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.FieldVersion;
import com.jivesoftware.os.tasmo.reference.lib.concur.FieldVersion;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathConsistencyException;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class ConcurrencyChecker {

    private final ConcurrencyStore concurrencyStore;

    public ConcurrencyChecker(ConcurrencyStore concurrencyStore) {
        this.concurrencyStore = concurrencyStore;
    }

    public long highestVersion(TenantIdAndCentricId tenantId, ObjectId instanceId, String refFieldName, long timestamp) {
        return concurrencyStore.highest(tenantId, instanceId, refFieldName, timestamp);
    }

    public List<Long> highestVersions(TenantIdAndCentricId tenantId, ObjectId instanceId, String[] refFieldNames) {
        return concurrencyStore.highests(tenantId, instanceId, refFieldNames);
    }

    public void checkIfModifiedOutFromUnderneathMe(TenantIdAndCentricId tenantIdAndCentricId,
            Set<FieldVersion> want) throws PathConsistencyException {
        Set<FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
        if (got != want) {
            PathConsistencyException e = new PathConsistencyException(want, got);
            throw e;
        }
    }

}

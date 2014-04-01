package com.jivesoftware.os.tasmo.lib.concur;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;
import java.util.List;

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

    public List<ConcurrencyStore.FieldVersion> checkIfModifiedOutFromUnderneathMe(TenantIdAndCentricId tenantIdAndCentricId,
            List<FieldVersion> want) throws PathModifiedOutFromUnderneathMeException {
        List<ConcurrencyStore.FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId, want);
        if (got != want) {
            PathModifiedOutFromUnderneathMeException e = new PathModifiedOutFromUnderneathMeException(want, got);
            throw e;
        }
        return want;
    }

}

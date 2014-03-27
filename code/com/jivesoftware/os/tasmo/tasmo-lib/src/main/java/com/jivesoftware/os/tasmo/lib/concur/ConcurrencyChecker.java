package com.jivesoftware.os.tasmo.lib.concur;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
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

    public long highestVersion(TenantId tenantId, ObjectId instanceId, String refFieldName, long timestamp) {
        return concurrencyStore.highest(tenantId, instanceId, refFieldName, timestamp);
    }

    public List<ConcurrencyStore.FieldVersion> checkIfModifiedOutFromUnderneathMe(TenantIdAndCentricId tenantIdAndCentricId,
            List<FieldVersion> want) throws PathModifiedOutFromUnderneathMeException {

//        Set<ObjectId> doTheseExist = new HashSet<>();
//        for (ConcurrencyStore.FieldVersion fieldVersion : want) {
//            doTheseExist.add(fieldVersion.getObjectId());
//        }
//        Set<ObjectId> exist = concurrencyStore.getExistence(tenantIdAndCentricId.getTenantId(), doTheseExist);
//        List<ConcurrencyStore.FieldVersion> wantWhatExists = new ArrayList<>();
//        for (ConcurrencyStore.FieldVersion fieldVersion : want) {
//            if (exist.contains(fieldVersion.getObjectId())) {
//                wantWhatExists.add(fieldVersion);
//            }
//        }

        List<ConcurrencyStore.FieldVersion> got = concurrencyStore.checkIfModified(tenantIdAndCentricId.getTenantId(), want);
        if (got != want) {
            PathModifiedOutFromUnderneathMeException e = new PathModifiedOutFromUnderneathMeException(want, got);
            throw e;
        }
        return want;
    }

}

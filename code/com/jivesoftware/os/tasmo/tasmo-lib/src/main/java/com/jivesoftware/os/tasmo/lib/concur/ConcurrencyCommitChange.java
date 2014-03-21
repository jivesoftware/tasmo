package com.jivesoftware.os.tasmo.lib.concur;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class ConcurrencyCommitChange implements CommitChange {

    private final ConcurrencyStore concurrencyStore;
    private final CommitChange commitChange;

    public ConcurrencyCommitChange(ConcurrencyStore concurrencyStore, CommitChange commitChange) {
        this.concurrencyStore = concurrencyStore;
        this.commitChange = commitChange;
    }

    @Override
    public void commitChange(TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
        System.out.println(Thread.currentThread() + " |--> ConcurrencyCommitChange:" + changes);
        commitChange.commitChange(tenantIdAndCentricId, changes);

        // TODO re-write to use batching!
        for (ViewFieldChange fieldChange : changes) {
            List<FieldVersion> expected = new ArrayList<>();
            List<ReferenceWithTimestamp> versions = fieldChange.getModelPathVersions();
            for (ReferenceWithTimestamp version : versions) {
                if (version != null) {
                    expected.add(new FieldVersion(version.getObjectId(), version.getFieldName(), version.getTimestamp()));
                }
            }
            List<FieldVersion> was = concurrencyStore.checkIfModified(tenantIdAndCentricId.getTenantId(), expected);
            if (expected != was) {
                PathModifiedOutFromUnderneathMeException pmofume = new PathModifiedOutFromUnderneathMeException(expected, was);
                //pmofume.printStackTrace();
                throw pmofume;
            }
        }
    }
}

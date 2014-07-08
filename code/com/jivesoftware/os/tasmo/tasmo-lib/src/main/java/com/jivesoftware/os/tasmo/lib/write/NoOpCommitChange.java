package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import java.util.List;

public class NoOpCommitChange implements CommitChange {

    @Override
    public void commitChange(WrittenEventContext batchContext, TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws
        CommitChangeException {
    }

}

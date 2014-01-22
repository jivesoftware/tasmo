/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ChangeTransaction {

    private final List<ViewFieldChange> changes = new ArrayList<>();
    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final CommitChange commitChange;

    public ChangeTransaction(TenantIdAndCentricId tenantIdAndCentricId, CommitChange commitChange) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.commitChange = commitChange;
    }

    public void change(ViewFieldChange change) throws CommitChangeException {
        if (!change.getTenantIdAndCentricId().equals(tenantIdAndCentricId)) {
            throw new CommitChangeException("tenant " + change.getTenantIdAndCentricId()
                + " does not match tenant " + tenantIdAndCentricId + " of transaction");
        }
        changes.add(change);
    }

    public List<ViewFieldChange> commit() throws CommitChangeException {
        try {
            commitChange.commitChange(tenantIdAndCentricId, changes);
            return changes;
        } catch (CommitChangeException ex) {
            throw new CommitChangeException("Error committing view field changes", ex);
        }
    }

    public boolean isEmpty() {
        return changes.isEmpty();
    }
}

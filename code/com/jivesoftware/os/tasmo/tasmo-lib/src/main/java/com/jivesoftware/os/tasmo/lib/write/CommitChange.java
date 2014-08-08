
package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import java.util.List;

/**
 *
 */
public interface CommitChange {

    void commitChange(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            List<ViewField> changes) throws CommitChangeException;
}

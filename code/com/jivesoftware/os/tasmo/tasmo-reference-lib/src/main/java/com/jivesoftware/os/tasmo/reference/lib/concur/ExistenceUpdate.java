package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;

public class ExistenceUpdate {

    public final TenantIdAndCentricId tenantId;
    public final long timestamp;
    public final ObjectId objectId;

    public ExistenceUpdate(TenantIdAndCentricId tenantId, long timestamp, ObjectId objectId) {
        this.tenantId = tenantId;
        this.timestamp = timestamp;
        this.objectId = objectId;
    }
}

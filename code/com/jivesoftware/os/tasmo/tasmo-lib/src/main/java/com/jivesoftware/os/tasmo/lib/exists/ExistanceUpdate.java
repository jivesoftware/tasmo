package com.jivesoftware.os.tasmo.lib.exists;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;

public class ExistanceUpdate {

    public final TenantId tenantId;
    public final long timestamp;
    public final ObjectId objectId;

    public ExistanceUpdate(TenantId tenantId, long timestamp, ObjectId objectId) {
        this.tenantId = tenantId;
        this.timestamp = timestamp;
        this.objectId = objectId;
    }
}

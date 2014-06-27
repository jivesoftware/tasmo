package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;

public interface TasmoEventPersistor {

    void removeValueFields(VersionedTasmoViewModel model, String className, TenantIdAndCentricId tenantIdAndCentricId, long timestamp, ObjectId instanceId);

    void updateValueFields(TenantIdAndCentricId tenantIdAndCentricId, long timestamp, ObjectId instanceId, VersionedTasmoViewModel model, String className,
        WrittenInstance writtenInstance) throws Exception;

}

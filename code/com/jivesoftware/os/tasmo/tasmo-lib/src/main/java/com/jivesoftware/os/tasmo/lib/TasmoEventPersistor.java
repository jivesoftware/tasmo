package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;

public interface TasmoEventPersistor {

    void removeValueFields(VersionedTasmoViewModel model,
        String className,
        TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId instanceId,
        long timestamp);

    void updateValueFields(VersionedTasmoViewModel model,
        String className,
        TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId instanceId,
        long timestamp,
        WrittenInstance writtenInstance) throws Exception;

}

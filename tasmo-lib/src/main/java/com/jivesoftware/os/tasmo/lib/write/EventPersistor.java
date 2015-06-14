package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.model.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;

public interface EventPersistor {

    void removeValueFields(VersionedTasmoViewModel model,
        String className,
        TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId instanceId,
        long timestamp) throws Exception;

    void updateValueFields(VersionedTasmoViewModel model,
        String className,
        TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId instanceId,
        long timestamp,
        WrittenInstance writtenInstance) throws Exception;

}

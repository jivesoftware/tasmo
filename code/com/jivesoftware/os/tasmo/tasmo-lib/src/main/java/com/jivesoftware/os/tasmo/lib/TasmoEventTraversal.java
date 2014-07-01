package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public interface TasmoEventTraversal {

    void traverseEvent(WrittenEventProcessor writtenEventProcessor, WrittenEventContext writtenEventContext, TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent) throws RuntimeException, Exception;

}

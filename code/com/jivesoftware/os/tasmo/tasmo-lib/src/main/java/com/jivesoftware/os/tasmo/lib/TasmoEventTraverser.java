package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public interface TasmoEventTraverser {

    void traverseEvent(InitiateWriteTraversal initiateTraversal, WrittenEventContext writtenEventContext, TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent) throws RuntimeException, Exception;

}

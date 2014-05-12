package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;

/**
 *
 * @author jonathan
 */
public interface StepStreamerFactory {

    StepStream create(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext);
}

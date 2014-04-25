package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;

/**
 *
 * @author jonathan
 */
public interface StepStreamerFactory {

    StepStream create(TenantIdAndCentricId tenantIdAndCentricId, PathTraversalContext context);
}

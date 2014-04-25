package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;

/**
 *
 * @author jonathan
 */
public class PrefixCollapsedStepStreamerFactory implements StepStreamerFactory {

    private final StepTree stepTree;

    public PrefixCollapsedStepStreamerFactory(StepTree stepTree) {
        this.stepTree = stepTree;
    }

    @Override
    public StepStream create(TenantIdAndCentricId tenantIdAndCentricId, PathTraversalContext context) {
        return new StepTreeStreamer(tenantIdAndCentricId, context, stepTree);
    }

}

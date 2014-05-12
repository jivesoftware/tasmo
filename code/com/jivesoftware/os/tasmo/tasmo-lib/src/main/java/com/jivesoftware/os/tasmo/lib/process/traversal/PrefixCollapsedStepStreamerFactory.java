package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;

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
    public StepStream create(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext) {
        return new StepTreeStreamer(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, stepTree);
    }

    @Override
    public String toString() {
        return "PrefixCollapsedStepStreamerFactory{" + "stepTree=" + stepTree + '}';
    }

}

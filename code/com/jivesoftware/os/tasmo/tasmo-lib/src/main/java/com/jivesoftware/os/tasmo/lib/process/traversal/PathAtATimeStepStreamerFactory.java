package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class PathAtATimeStepStreamerFactory implements StepStreamerFactory {

    private final List<StepTraverser> stepTraversers;

    public PathAtATimeStepStreamerFactory(List<StepTraverser> stepTraversers) {
        this.stepTraversers = stepTraversers;
    }

    @Override
    public StepStreamer create(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext) {

        return new StepStreamer(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, stepTraversers, 0);
    }

}

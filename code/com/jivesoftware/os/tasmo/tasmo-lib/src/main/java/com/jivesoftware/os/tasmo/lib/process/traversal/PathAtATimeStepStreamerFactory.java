package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
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
    public StepStreamer create(TenantIdAndCentricId tenantIdAndCentricId, PathTraversalContext context) {
        return new StepStreamer(tenantIdAndCentricId, context, stepTraversers, 0);
    }

}

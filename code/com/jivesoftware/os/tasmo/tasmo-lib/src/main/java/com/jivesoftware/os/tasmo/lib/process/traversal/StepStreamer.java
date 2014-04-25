package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class StepStreamer implements StepStream {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final PathTraversalContext context;
    private final List<StepTraverser> steps;
    private final int stepIndex;

    public StepStreamer(TenantIdAndCentricId tenantIdAndCentricId,
            PathTraversalContext context,
            List<StepTraverser> steps,
            int stepIndex) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.context = context;
        this.steps = steps;
        this.stepIndex = stepIndex;
    }

    @Override
    public void stream(PathId pathId) throws Exception {
        steps.get(stepIndex).process(tenantIdAndCentricId, context, pathId, nextStepStreamer());
    }

    private StepStreamer nextStepStreamer() {
        return new StepStreamer(tenantIdAndCentricId, context, steps, stepIndex + 1);
    }

}

package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.List;

/**
 *
 * @author jonathan
 */
class StepStreamer implements StepStream {

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
    public void stream(ReferenceWithTimestamp reference) throws Exception {
        steps.get(stepIndex).process(tenantIdAndCentricId, context, reference, nextStepStreamer());
    }

    @Override
    public int getStepIndex() {
        return stepIndex;
    }

    private StepStreamer nextStepStreamer() {
        return new StepStreamer(tenantIdAndCentricId, context, steps, stepIndex + 1);
    }

}

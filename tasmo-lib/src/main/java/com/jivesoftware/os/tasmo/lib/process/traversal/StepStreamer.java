package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class StepStreamer implements StepStream {

    private final List<StepTraverser> steps;
    private final int stepIndex;

    public StepStreamer(
            List<StepTraverser> steps,
            int stepIndex) {
        this.steps = steps;
        this.stepIndex = stepIndex;
    }

    @Override
    public void stream(TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext,
            PathId pathId) throws Exception {
        steps.get(stepIndex).process(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext, pathId, nextStepStreamer());
    }

    private StepStreamer nextStepStreamer() {
        return new StepStreamer(steps, stepIndex + 1);
    }

}

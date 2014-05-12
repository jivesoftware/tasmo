package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class StepStreamer implements StepStream {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final WrittenEventContext writtenEventContext;
    private final PathTraversalContext context;
    private final PathContext pathContext;
    private final LeafContext leafContext;
    private final List<StepTraverser> steps;
    private final int stepIndex;

    public StepStreamer(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext,
            List<StepTraverser> steps,
            int stepIndex) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.writtenEventContext = writtenEventContext;
        this.context = context;
        this.pathContext = pathContext;
        this.leafContext = leafContext;
        this.steps = steps;
        this.stepIndex = stepIndex;
    }

    @Override
    public void stream(PathId pathId) throws Exception {
        steps.get(stepIndex).process(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, pathId, nextStepStreamer());
    }

    private StepStreamer nextStepStreamer() {
        return new StepStreamer(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, steps, stepIndex + 1);
    }

}

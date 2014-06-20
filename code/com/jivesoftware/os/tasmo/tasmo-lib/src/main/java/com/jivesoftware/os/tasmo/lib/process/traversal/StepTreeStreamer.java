package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;

/**
 *
 * @author jonathan
 */
public class StepTreeStreamer implements StepStream {

    private final StepTree stepTree;

    public StepTreeStreamer(StepTree stepTree) {
        this.stepTree = stepTree;
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext,
            PathId pathId) throws Exception {
        for (StepTraverser stepTraverser : stepTree.map.keySet()) {
            StepTree nextStepTree = stepTree.map.get(stepTraverser);
            stepTraverser.process(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, pathId, new StepTreeStreamer(nextStepTree));
        }
    }
}

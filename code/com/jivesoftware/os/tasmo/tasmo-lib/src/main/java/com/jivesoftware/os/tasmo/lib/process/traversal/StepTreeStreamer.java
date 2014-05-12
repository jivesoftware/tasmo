package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;

/**
 *
 * @author jonathan
 */
public class StepTreeStreamer implements StepStream {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final WrittenEventContext writtenEventContext;
    private final PathTraversalContext context;
    private final PathContext pathContext;
    private final LeafContext leafContext;
    private final StepTree stepTree;

    public StepTreeStreamer(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext,
            StepTree stepTree) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.writtenEventContext  = writtenEventContext;
        this.context = context;
        this.pathContext = pathContext;
        this.leafContext = leafContext;
        this.stepTree = stepTree;
    }

    @Override
    public void stream(PathId pathId) throws Exception {
        for (StepTraverser stepTraverser : stepTree.map.keySet()) {
            StepTree nextStepTree = stepTree.map.get(stepTraverser);
            stepTraverser.process(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, pathId,
                    new StepTreeStreamer(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, nextStepTree));
        }
    }
}

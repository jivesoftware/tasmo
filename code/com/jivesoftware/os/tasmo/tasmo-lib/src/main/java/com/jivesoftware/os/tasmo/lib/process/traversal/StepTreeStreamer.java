package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.write.PathId;

/**
 *
 * @author jonathan
 */
public class StepTreeStreamer implements StepStream {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final PathTraversalContext context;
    private final StepTree stepTree;

    public StepTreeStreamer(TenantIdAndCentricId tenantIdAndCentricId,
            PathTraversalContext context,
            StepTree stepTree) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.context = context;
        this.stepTree = stepTree;
    }

    @Override
    public void stream(PathId pathId) throws Exception {
        for (StepTraverser stepTraverser : stepTree.map.keySet()) {
            StepTree nextStepTree = stepTree.map.get(stepTraverser);
            stepTraverser.process(tenantIdAndCentricId, context, pathId, new StepTreeStreamer(tenantIdAndCentricId, context, nextStepTree));
        }
    }
}

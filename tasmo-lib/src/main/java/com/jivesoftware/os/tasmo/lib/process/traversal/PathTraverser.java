package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.Set;

public class PathTraverser {

    private final PathTraverserKey pathTraverserKey;
    private final StepStreamerFactory streamerFactory;

    public PathTraverser(PathTraverserKey pathTraverserKey, StepStreamerFactory streamerFactory) {
        this.pathTraverserKey = pathTraverserKey;
        this.streamerFactory = streamerFactory;
    }

    public PathTraverserKey getPathTraverserKey() {
        return pathTraverserKey;
    }

    public PathTraversalContext createContext(WrittenEventContext writtenEventContext,
            WrittenEvent writtenEvent,
            long threadTimestamp,
            boolean removalContext) {

        PathTraversalContext context = new PathTraversalContext(
                threadTimestamp,
                removalContext);
        return context;
    }

    public PathContext createPathContext() {
        return new PathContext(pathTraverserKey.getPathLength());
    }

    public Set<String> getInitialFieldNames() {
        return pathTraverserKey.getInitialFieldNames();
    }

    public boolean isCentric() {
        return pathTraverserKey.isCentric();
    }

    public int getPathIndex() {
        return pathTraverserKey.getPathIndex();
    }

    public void traverse(TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext context,
            PathContext pathContext,
            LeafContext leafContext,
            PathId pathId) throws Exception {
        StepStream stepStream = streamerFactory.create();
        stepStream.stream(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext, pathId);
    }

    @Override
    public String toString() {
        return "PathTraverser{" + "pathTraverserKey=" + pathTraverserKey + ", streamerFactory=" + streamerFactory + '}';
    }

}

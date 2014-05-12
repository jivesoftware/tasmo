package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class PathContext {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PathId[] modelPathInstanceIds;
    private final long[] modelPathTimestamps;
    private final List<ReferenceWithTimestamp>[] modelPathVersionState;
    private int lastPathIndex = 0;

    public PathContext(int numberOfPathIds) {
        this.modelPathInstanceIds = new PathId[numberOfPathIds];
        this.modelPathTimestamps = new long[numberOfPathIds + 1];
        this.modelPathVersionState = new List[numberOfPathIds];
        for (int i = 0; i < numberOfPathIds; i++) {
            modelPathVersionState[i] = new ArrayList<>();
        }
    }

    public void setPathId(WrittenEventContext writtenEventContext, int pathIndex, ObjectId id, long timestamp) {
        this.modelPathInstanceIds[pathIndex] = new PathId(id, timestamp);
        this.modelPathTimestamps[pathIndex] = timestamp;
        if (pathIndex == lastPathIndex) {
            writtenEventContext.fanBreath++;
        } else {
            writtenEventContext.fanDepth++;
        }
        lastPathIndex = pathIndex;
    }

    public void addVersions(int pathIndex, Collection<ReferenceWithTimestamp> versions) {
        this.modelPathVersionState[pathIndex] = new ArrayList<>(versions);
    }

    public PathId[] copyOfModelPathInstanceIds() {
        return Arrays.copyOf(modelPathInstanceIds, modelPathInstanceIds.length);
    }

    public long[] copyOfModelPathTimestamps() {
        return Arrays.copyOf(modelPathTimestamps, modelPathTimestamps.length);
    }

    public List<ReferenceWithTimestamp> copyOfVersions() {
        List<ReferenceWithTimestamp> copy = new ArrayList<>();
        for (List<ReferenceWithTimestamp> versiosns : modelPathVersionState) {
            copy.addAll(versiosns);
        }
        return copy;
    }

    public PathId getPathId(int pathIndex) {
        return this.modelPathInstanceIds[pathIndex];
    }

    void setLastTimestamp(long latestTimestamp) {
        modelPathTimestamps[modelPathTimestamps.length - 1] = latestTimestamp;
    }
}

package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.lib.write.PathId;

/**
 *
 * @author jonathan.colt
 */
public interface StepStream {

    void stream(PathId pathId) throws Exception; // TODO: Consider batching?

}

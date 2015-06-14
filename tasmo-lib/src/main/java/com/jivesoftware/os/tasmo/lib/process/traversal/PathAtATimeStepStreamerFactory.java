package com.jivesoftware.os.tasmo.lib.process.traversal;

import java.util.List;

/**
 *
 * @author jonathan
 */
public class PathAtATimeStepStreamerFactory implements StepStreamerFactory {

    private final List<StepTraverser> stepTraversers;

    public PathAtATimeStepStreamerFactory(List<StepTraverser> stepTraversers) {
        this.stepTraversers = stepTraversers;
    }

    @Override
    public StepStreamer create() {

        return new StepStreamer(stepTraversers, 0);
    }

    @Override
    public String toString() {
        return "PathAtATimeStepStreamerFactory{" + "stepTraversers=" + stepTraversers + '}';
    }
}

package com.jivesoftware.os.tasmo.lib.process.traversal;

/**
 *
 * @author jonathan
 */
public class PrefixCollapsedStepStreamerFactory implements StepStreamerFactory {

    private final StepTree stepTree;

    public PrefixCollapsedStepStreamerFactory(StepTree stepTree) {
        this.stepTree = stepTree;
    }

    @Override
    public StepStream create() {
        return new StepTreeStreamer(stepTree);
    }

    @Override
    public String toString() {
        return "PrefixCollapsedStepStreamerFactory{" + "stepTree=" + stepTree + '}';
    }

}

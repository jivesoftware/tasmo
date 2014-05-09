package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class TraversablePath {

    private final InitiateTraversalContext initialStepContext;
    private final List<StepTraverser> stepTraversers;

    public TraversablePath(InitiateTraversalContext initialStepContext,
            List<StepTraverser> stepTraversers) {
        this.initialStepContext = initialStepContext;
        this.stepTraversers = stepTraversers;
    }

    public List<String> getInitialFieldNames() {
        return initialStepContext.getInitialFieldNames();
    }

    public ModelPathStepType getInitialModelPathStepType() {
        return initialStepContext.getInitialModelPathStepType();
    }

    public String getRefFieldName() {
        return initialStepContext.getRefFieldName();
    }

    public Iterable<String> getInitialClassNames() {
        return initialStepContext.getInitialClassNames();
    }

    public InitiateTraversalContext getInitialStepContext() {
        return initialStepContext;
    }

    public List<StepTraverser> getStepTraversers() {
        return stepTraversers;
    }

    @Override
    public String toString() {
        return "TraversablePath{" + "initialStepContext=" + initialStepContext + ", stepTraversers=" + stepTraversers + '}';
    }
}

/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.Lists;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.reference.lib.BackRefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.ForwardRefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class PathTraversersFactory {

    private final String viewClassName;
    private final long modelPathIdHashcode;
    private final ModelPath modelPath;

    public PathTraversersFactory(
            String viewClassName,
            long modelPathIdHashcode,
            ModelPath modelPath) {
        this.viewClassName = viewClassName;
        this.modelPathIdHashcode = modelPathIdHashcode;
        this.modelPath = modelPath;
    }

    public List<StepTraverser> buildReadSteps(String viewIdFieldName) {
        List<StepTraverser> steps = new ArrayList<>();
        List<ModelPathStep> modelPathSteps = modelPath.getPathMembers();
        steps.addAll(buildLeafwardTraversers(-1, modelPathSteps));
        steps.addAll(buildRootwardTraversers(-1, modelPathSteps));
        steps.add(new TraverseViewValueWriter(viewIdFieldName, viewClassName, modelPath, modelPathIdHashcode));
        return steps;
    }

    public List<TraversablePath> buildPathTraversers(String viewIdFieldName) {
        List<TraversablePath> pathTraversers = Lists.newArrayList();
        List<ModelPathStep> modelPathMembers = modelPath.getPathMembers();
        for (int i = 0; i < modelPathMembers.size(); i++) {
            pathTraversers.add(buildPathTraverser(viewIdFieldName, modelPathMembers, i));
        }
        return pathTraversers;
    }

    private TraversablePath buildPathTraverser(String viewIdFieldName,
            List<ModelPathStep> modelPathSteps,
            int initialPathIndex) {

        InitiateTraversalContext firstStep = new InitiateTraversalContext(
                modelPathSteps.get(initialPathIndex),
                initialPathIndex,
                modelPathSteps.size(),
                viewClassName,
                modelPathIdHashcode);

        List<StepTraverser> steps = new ArrayList<>();
        steps.addAll(buildLeafwardTraversers(initialPathIndex, modelPathSteps));
        steps.addAll(buildRootwardTraversers(initialPathIndex, modelPathSteps));
        steps.add(new TraverseViewValueWriter(viewIdFieldName, viewClassName, modelPath, modelPathIdHashcode));
        return new TraversablePath(firstStep, steps);
    }

    public List<TraversablePath> buildBackPathTraversers(String viewIdFieldName) {
        List<TraversablePath> pathTraversers = Lists.newArrayList();
        List<ModelPathStep> modelPathMembers = modelPath.getPathMembers();
        for (int i = 0; i < modelPathMembers.size(); i++) {
            TraversablePath buildInitialBackrefStep = buildInitialBackrefStep(viewIdFieldName, i);
            if (buildInitialBackrefStep != null) {
                pathTraversers.add(buildInitialBackrefStep);
            }
        }
        return pathTraversers;
    }

    private TraversablePath buildInitialBackrefStep(String viewIdFieldName, int initialPathIndex) {
        List<ModelPathStep> modelPathSteps = modelPath.getPathMembers();

        ModelPathStep modelPathStep = modelPathSteps.get(initialPathIndex);
        ModelPathStepType stepType = modelPathStep.getStepType();
        if (stepType.isBackReferenceType()) {

            int modelPathMembersSize = modelPathSteps.size();
            InitiateTraversalContext firstStep = new InitiateTraversalContext(
                    modelPathStep,
                    initialPathIndex,
                    modelPathMembersSize,
                    viewClassName,
                    modelPathIdHashcode) {
                        @Override
                        public String getRefFieldName() {
                            return null;
                        }

                        @Override
                        public Set<String> getInitialClassNames() {
                            return super.getInitialModelPathStep().getDestinationClassNames();
                        }

                        @Override
                        public ModelPathStepType getInitialModelPathStepType() {
                            return ModelPathStepType.value;
                        }
                    };

            List<StepTraverser> steps = new ArrayList<>();
            steps.add(new TraverseBackref(modelPathStep, modelPathStep.getOriginClassNames(), modelPathStep.getStepType().isCentric()));
            steps.addAll(buildLeafwardTraversers(initialPathIndex, modelPathSteps));
            steps.addAll(buildRootwardTraversers(initialPathIndex, modelPathSteps));
            steps.add(new TraverseViewValueWriter(viewIdFieldName, viewClassName, modelPath, modelPathIdHashcode));
            return new TraversablePath(firstStep, steps);
        } else {
            return null;
        }
    }

    private List<StepTraverser> buildLeafwardTraversers(int initialPathIndex, List<ModelPathStep> modelPathMembers) {
        List<StepTraverser> steps = new ArrayList<>();

        int modelPathMembersSize = modelPathMembers.size();
        ModelPathStep member;
        ModelPathStepType memberType;
        // leafward
        for (int pathIndex = initialPathIndex + 1; pathIndex < modelPathMembersSize; pathIndex++) {
            member = modelPathMembers.get(pathIndex);
            boolean centric = member.getStepType().isCentric();

            StepTraverser processStep;
            if (pathIndex == modelPathMembersSize - 1) {

                processStep = new TraverseValue(new HashSet<>(member.getFieldNames()), initialPathIndex, pathIndex, centric);

            } else {
                memberType = member.getStepType();
                RefStreamer streamer = createLeafwardStreamer(
                        member.getOriginClassNames(),
                        member.getRefFieldName(),
                        memberType);

                Set<String> streamToTypes = memberType.isBackReferenceType() ? member.getOriginClassNames() : member.getDestinationClassNames();
                processStep = new TraverseLeafward(streamer, pathIndex, streamToTypes, centric);
            }

            steps.add(processStep);
        }
        return steps;
    }

    private RefStreamer createLeafwardStreamer(Set<String> classNames, String fieldName, ModelPathStepType fieldType) {
        switch (fieldType) {
            case ref:
                return new ForwardRefStreamer(fieldName);
            case refs:
                return new ForwardRefStreamer(fieldName);
            case backRefs:
            case count:
            case latest_backRef:
                return new BackRefStreamer(classNames, fieldName);
            default:
                throw new IllegalArgumentException("fieldType:" + fieldType + " doesn't support rev streaming");
        }
    }

    private List<StepTraverser> buildRootwardTraversers(int initialPathIndex, List<ModelPathStep> modelPathMembers) {
        List<StepTraverser> steps = new ArrayList<>();
        ModelPathStep member;
        // rootward
        for (int pathIndex = initialPathIndex - 1; pathIndex >= 0; pathIndex--) {
            member = modelPathMembers.get(pathIndex);
            boolean centric = member.getStepType().isCentric();
            RefStreamer streamer = createRootwardStreamer(
                    member.getOriginClassNames(),
                    member.getRefFieldName(),
                    member.getStepType());

            Set<String> streamToTypes = member.getStepType().isBackReferenceType() ? member.getDestinationClassNames() : member.getOriginClassNames();
            StepTraverser processStep = new TraverseRootward(streamer, pathIndex, streamToTypes, centric);
            steps.add(processStep);
        }
        return steps;
    }

    private RefStreamer createRootwardStreamer(Set<String> classNames, String fieldName, ModelPathStepType fieldType) {
        switch (fieldType) {
            case ref:
                return new BackRefStreamer(classNames, fieldName);
            case refs:
                return new BackRefStreamer(classNames, fieldName);
            case backRefs:
            case count:
            case latest_backRef: // For this case we are likely doing more work that we absolutely need to.
                return new ForwardRefStreamer(fieldName);
            default:
                throw new IllegalArgumentException("fieldType:" + fieldType + " doesn't support fwd streaming");
        }
    }
}

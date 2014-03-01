/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.google.common.collect.Lists;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.reference.lib.A_IdsStreamer;
import com.jivesoftware.os.tasmo.reference.lib.B_IdsStreamer;
import com.jivesoftware.os.tasmo.reference.lib.Latest_A_IdStreamer;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class FieldProcessorFactory {

    private final String viewClassName;
    private final String modelPathId;
    private final ModelPath modelPath;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;

    public FieldProcessorFactory(
        String viewClassName,
        ModelPath modelPath,
        EventValueStore eventValueStore,
        ReferenceStore referenceStore) {
        this.viewClassName = viewClassName;
        this.modelPathId = modelPath.getId();
        this.modelPath = modelPath;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
    }

    public List<FieldProcessor> buildFieldProcessors(FieldProcessorConfig executableStepConfig) {
        List<FieldProcessor> initialExecutableSteps = Lists.newArrayList();
        List<ModelPathStep> modelPathMembers = modelPath.getPathMembers();
        for (int i = 0; i < modelPathMembers.size(); i++) {
            initialExecutableSteps.add(buildFieldProcessor(executableStepConfig, modelPathMembers, i));
        }
        return initialExecutableSteps;
    }

    private FieldProcessor buildFieldProcessor(FieldProcessorConfig executableStepConfig, List<ModelPathStep> modelPathMembers, int initialPathIndex) {

        List<ProcessStep> steps = new ArrayList<>();
        InitialStepContext firstStep = new InitialStepContext(
            modelPathMembers.get(initialPathIndex),
            initialPathIndex,
            modelPathMembers.size(),
            viewClassName,
            modelPathId);

        steps.addAll(buildLeafwardSteps(initialPathIndex, modelPathMembers));
        steps.addAll(buildRootwardSteps(initialPathIndex, modelPathMembers));
        steps.add(new ViewValueWriterStep(viewClassName, modelPathId));

        return new FieldProcessor(firstStep, steps, executableStepConfig);
    }

    public FieldProcessor buildInitialBackrefStep(FieldProcessorConfig executableStepConfig) {
        List<ModelPathStep> modelPathMembers = modelPath.getPathMembers();

        ModelPathStep modelPathStep = modelPathMembers.get(0);
        ModelPathStepType stepType = modelPathStep.getStepType();
        if (stepType.isBackReferenceType()) {

            int modelPathMembersSize = modelPathMembers.size();
            InitialStepContext firstStep = new InitialStepContext(
                modelPathStep,
                0,
                modelPathMembersSize,
                viewClassName,
                modelPathId) {
                @Override
                public String getRefFieldName() {
                    return null;
                }

                @Override
                public Set<String> getInitialClassNames() {
                    return super.initialModelPathMember.getDestinationClassNames();
                }

                @Override
                public ModelPathStepType getInitialModelPathStepType() {
                    return ModelPathStepType.value;
                }
            };

            List<ProcessStep> steps = new ArrayList<>();
            steps.add(new BackrefStep(modelPathStep, referenceStore, modelPathStep.getOriginClassNames()));
            steps.addAll(buildLeafwardSteps(0, modelPathMembers));
            steps.addAll(buildRootwardSteps(0, modelPathMembers));
            steps.add(new ViewValueWriterStep(viewClassName, modelPathId));

            return new FieldProcessor(firstStep, steps, executableStepConfig);
        } else {
            return null;
        }
    }

    List<ProcessStep> buildLeafwardSteps(int initialPathIndex, List<ModelPathStep> modelPathMembers) {
        List<ProcessStep> steps = new ArrayList<>();

        int modelPathMembersSize = modelPathMembers.size();
        ModelPathStep member;
        ModelPathStepType memberType;
        // leafward
        for (int pathIndex = initialPathIndex + 1; pathIndex < modelPathMembersSize; pathIndex++) {
            member = modelPathMembers.get(pathIndex);

            ProcessStep processStep;
            if (pathIndex == modelPathMembersSize - 1) {

                processStep = new ValueStep(eventValueStore, member.getFieldNames(), initialPathIndex, pathIndex);

            } else {
                memberType = member.getStepType();
                RefStreamer streamer = createLeafwardStreamer(
                    member.getOriginClassNames(),
                    member.getRefFieldName(),
                    memberType);

                Set<String> streamToTypes =
                    memberType.isBackReferenceType() ? member.getOriginClassNames() : member.getDestinationClassNames();
                processStep = new LeafwardStep(streamer, pathIndex, streamToTypes);
            }

            steps.add(processStep);
        }
        return steps;
    }

    RefStreamer createLeafwardStreamer(Set<String> aClassNames, String aFieldName, ModelPathStepType fieldType) {
        switch (fieldType) {
            case ref:
                return new B_IdsStreamer(referenceStore, aFieldName);
            case refs:
                return new B_IdsStreamer(referenceStore, aFieldName);
            case backRefs:
            case count:
                return new A_IdsStreamer(referenceStore, aClassNames, aFieldName);
            case latest_backRef:
                return new Latest_A_IdStreamer(referenceStore, aClassNames, aFieldName);
            default:
                throw new IllegalArgumentException("fieldType:" + fieldType + " doesn't support rev streaming");
        }
    }

    List<ProcessStep> buildRootwardSteps(int initialPathIndex, List<ModelPathStep> modelPathMembers) {
        List<ProcessStep> steps = new ArrayList<>();
        ModelPathStep member;
        // rootward
        for (int pathIndex = initialPathIndex - 1; pathIndex >= 0; pathIndex--) {
            member = modelPathMembers.get(pathIndex);
            RefStreamer streamer = createRootwardStreamer(
                member.getOriginClassNames(),
                member.getRefFieldName(),
                member.getStepType());

            Set<String> streamToTypes =
                member.getStepType().isBackReferenceType() ? member.getDestinationClassNames() : member.getOriginClassNames();
            ProcessStep processStep = new RootwardStep(streamer, pathIndex, streamToTypes);
            steps.add(processStep);
        }
        return steps;
    }

    RefStreamer createRootwardStreamer(Set<String> aClassNames, String aFieldName, ModelPathStepType fieldType) {
        switch (fieldType) {
            case ref:
                return new A_IdsStreamer(referenceStore, aClassNames, aFieldName);
            case refs:
                return new A_IdsStreamer(referenceStore, aClassNames, aFieldName);
            case backRefs:
            case count:
            case latest_backRef: // For this case we are likely doing more work that we absolutely need to.
                return new B_IdsStreamer(referenceStore, aFieldName);
            default:
                throw new IllegalArgumentException("fieldType:" + fieldType + " doesn't support fwd streaming");
        }
    }
}

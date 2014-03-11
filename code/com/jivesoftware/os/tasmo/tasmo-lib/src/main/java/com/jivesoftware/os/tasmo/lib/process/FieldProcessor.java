package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.List;

public class FieldProcessor {

    private final InitialStepContext initialStep;
    private final List<ProcessStep> steps;
    private final FieldProcessorConfig executableStepConfig;

    public FieldProcessor(InitialStepContext initialStep, List<ProcessStep> steps, FieldProcessorConfig executableStepConfig) {
        this.initialStep = initialStep;
        this.steps = steps;
        this.executableStepConfig = executableStepConfig;
    }

    public ViewFieldContext createContext(ModifiedViewProvider modifiedViewProvider,
        Reference objectInstanceId,
        TenantId tenantId,
        Id actorId,
        Id centricId) {
        return createContext(modifiedViewProvider, initialStep, objectInstanceId, tenantId, actorId, centricId);
    }

    public void process(TenantIdAndCentricId tenantIdAndCentricId,
        ViewFieldContext context,
        Reference objectInstanceId) throws Exception {

        StepStreamer stepStreamer = new StepStreamer(tenantIdAndCentricId, context, steps, 0);
        stepStreamer.stream(objectInstanceId);

    }

    private static class StepStreamer implements StepStream {

        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final ViewFieldContext context;
        private final List<ProcessStep> steps;
        private final int stepIndex;

        StepStreamer(TenantIdAndCentricId tenantIdAndCentricId, ViewFieldContext context,
            List<ProcessStep> steps, int stepIndex) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.context = context;
            this.steps = steps;
            this.stepIndex = stepIndex;
        }


        @Override
        public void stream(Reference reference) throws Exception {
            ProcessStep got = steps.get(stepIndex);
            got.process(tenantIdAndCentricId, context, reference, nextStepStreamer());
        }

        @Override
        public int getStepIndex() {
            return stepIndex;
        }

        private StepStreamer nextStepStreamer() {
            return new StepStreamer(tenantIdAndCentricId, context, steps, stepIndex + 1);
        }

    }

    public List<String> getInitialFieldNames() {
        return initialStep.getInitialFieldNames();
    }

    public ModelPathStep getModelPathStep() {
        return initialStep.initialModelPathMember;
    }

    private ViewFieldContext createContext(
        final ModifiedViewProvider modifiedViewProvider,
        InitialStepContext step,
        Reference objectInstanceId,
        TenantId tenantId,
        Id actorId,
        Id centricId) {

        Id userId = Id.NULL;
        if (centricId != null) {
            userId = centricId;
        }

        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);

        CommitChange notificationsAfterCommitingChanges = executableStepConfig.commitChange;
        if (executableStepConfig.notificationRequired) {
            notificationsAfterCommitingChanges = new CommitChange() {
                @Override
                public void commitChange(TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
                    executableStepConfig.commitChange.commitChange(tenantIdAndCentricId, changes);
                    for (ViewFieldChange viewFieldChange : changes) {
                        ModifiedViewInfo modifiedViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, viewFieldChange.getViewObjectId());
                        modifiedViewProvider.add(modifiedViewInfo);
                    }
                }
            };
        }
        ViewFieldContext context = new ViewFieldContext(
            tenantIdAndCentricId,
            actorId,
            executableStepConfig.writtenEventProvider,
            notificationsAfterCommitingChanges,
            step.getMembersSize());
        context.setPathId(0, objectInstanceId);
        return context;
    }


    public ModelPathStepType getInitialModelPathStepType() {
        return initialStep.getInitialModelPathStepType();
    }

    public String getRefFieldName() {
        return initialStep.getRefFieldName();
    }

    public Iterable<String> getInitialClassNames() {
        return initialStep.getInitialClassNames();
    }
}

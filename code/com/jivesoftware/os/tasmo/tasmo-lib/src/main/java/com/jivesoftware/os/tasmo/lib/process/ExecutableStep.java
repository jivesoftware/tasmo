package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.List;

public class ExecutableStep {

    private final InitialStepContext initialStep;
    private final List<ProcessStep> steps;
    private final ExecutableStepConfig executableStepConfig;

    public ExecutableStep(InitialStepContext initialStep, List<ProcessStep> steps, ExecutableStepConfig executableStepConfig) {
        this.initialStep = initialStep;
        this.steps = steps;
        this.executableStepConfig = executableStepConfig;
    }

    public ViewFieldContext createContext(ModifiedViewProvider modifiedViewProvider,
        WrittenEvent writtenEvent,
        Reference objectInstanceId,
        boolean removalContext) {
        return createContext(modifiedViewProvider, writtenEvent, initialStep, objectInstanceId, removalContext);
    }

    public void process(TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent,
        ViewFieldContext context,
        Reference objectInstanceId) throws Exception {

        StepStreamer stepStreamer = new StepStreamer(tenantIdAndCentricId, writtenEvent, context, steps, 0);
        stepStreamer.stream(objectInstanceId);

    }

    private static class StepStreamer implements StepStream {

        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final WrittenEvent writtenEvent;
        private final ViewFieldContext context;
        private final List<ProcessStep> steps;
        private final int stepIndex;

        StepStreamer(TenantIdAndCentricId tenantIdAndCentricId, WrittenEvent writtenEvent, ViewFieldContext context,
            List<ProcessStep> steps, int stepIndex) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.writtenEvent = writtenEvent;
            this.context = context;
            this.steps = steps;
            this.stepIndex = stepIndex;
        }


        @Override
        public void stream(Reference reference) throws Exception {
            ProcessStep got = steps.get(stepIndex);
            got.process(tenantIdAndCentricId, writtenEvent, context, reference, nextStepStreamer());
        }

        @Override
        public int getStepIndex() {
            return stepIndex;
        }

        private StepStreamer nextStepStreamer() {
            return new StepStreamer(tenantIdAndCentricId, writtenEvent, context, steps, stepIndex + 1);
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
        WrittenEvent writtenEvent,
        InitialStepContext step,
        Reference objectInstanceId,
        boolean removalContext) {

        TenantId tenantId = writtenEvent.getTenantId();
        Id userId = Id.NULL;
        if (executableStepConfig.idCentric) {
            userId = writtenEvent.getCentricId();
        }

        Id alternateViewId = buildAlternateViewId(writtenEvent);
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
        ViewFieldContext context = new ViewFieldContext(writtenEvent.getEventId(),
            tenantIdAndCentricId,
            writtenEvent.getActorId(),
            executableStepConfig.writtenEventProvider,
            notificationsAfterCommitingChanges,
            writtenEvent.getEventId(),
            alternateViewId,
            step.getMembersSize(),
            removalContext);
        context.setPathId(step.getPathIndex(), objectInstanceId);
        return context;
    }

    protected Id buildAlternateViewId(WrittenEvent writtenEvent) throws IllegalStateException {
        Id alternateViewId = null;
        if (executableStepConfig.viewIdFieldName != null) {
            WrittenInstance payload = writtenEvent.getWrittenInstance();

            if (payload.hasField(executableStepConfig.viewIdFieldName)) {
                try {
                    // We are currently only supporting one ref, but with light change we could support a list of refs. Should we?
                    ObjectId objectId = payload.getReferenceFieldValue(executableStepConfig.viewIdFieldName);
                    if (objectId != null) {
                        alternateViewId = objectId.getId();
                    }
                    alternateViewId = writtenEvent.getWrittenInstance().getIdFieldValue(executableStepConfig.viewIdFieldName);
                } catch (Exception x) {
                    throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                        + executableStepConfig.viewIdFieldName
                        + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.", x);
                }
            } else {
                throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                    + executableStepConfig.viewIdFieldName
                    + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.");
            }
        }
        return alternateViewId;
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

package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.List;

public class PathTraverser {

    private final InitiateTraversalContext initialStepContext;
    private final List<StepTraverser> stepTraversers;
    private final PathTraverserConfig pathTraverserConfig;

    public PathTraverser(InitiateTraversalContext initialStepContext,
            List<StepTraverser> stepTraversers,
            PathTraverserConfig pathTraverserConfig) {
        this.initialStepContext = initialStepContext;
        this.stepTraversers = stepTraversers;
        this.pathTraverserConfig = pathTraverserConfig;
    }

    public PathTraversalContext createContext(WrittenEventContext writtenEventContext,
            WrittenEvent writtenEvent,
            long threadTimestamp,
            boolean removalContext) {
        return createContext(writtenEventContext, writtenEvent, initialStepContext, threadTimestamp, removalContext);
    }

    public void travers(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent,
            PathTraversalContext context,
            PathId pathId) throws Exception {

        StepStreamer stepStreamer = new StepStreamer(tenantIdAndCentricId, context, stepTraversers, 0);
        stepStreamer.stream(pathId);

    }

    public List<String> getInitialFieldNames() {
        return initialStepContext.getInitialFieldNames();
    }

    public ModelPathStep getModelPathStep() {
        return initialStepContext.initialModelPathMember;
    }

    private PathTraversalContext createContext(
            final WrittenEventContext writtenEventContext,
            WrittenEvent writtenEvent,
            InitiateTraversalContext step,
            long threadTimestamp,
            boolean removalContext) {


        Id alternateViewId = buildAlternateViewId(writtenEvent);
        CommitChange notificationsAfterCommitingChanges = pathTraverserConfig.commitChange;
        if (pathTraverserConfig.notificationRequired) {
            notificationsAfterCommitingChanges = new CommitChange() {
                @Override
                public void commitChange(TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
                    pathTraverserConfig.commitChange.commitChange(tenantIdAndCentricId, changes);
                    for (ViewFieldChange viewFieldChange : changes) {
                        ModifiedViewInfo modifiedViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, viewFieldChange.getViewObjectId());
                        writtenEventContext.getModifiedViewProvider().add(modifiedViewInfo);
                    }
                }
            };
        }
        PathTraversalContext context = new PathTraversalContext(writtenEvent,
                pathTraverserConfig.writtenEventProvider,
                notificationsAfterCommitingChanges,
                alternateViewId,
                step.getMembersSize(),
                threadTimestamp,
                removalContext);
        return context;
    }

    protected Id buildAlternateViewId(WrittenEvent writtenEvent) throws IllegalStateException {
        Id alternateViewId = null;
        if (pathTraverserConfig.viewIdFieldName != null) {
            WrittenInstance payload = writtenEvent.getWrittenInstance();

            if (payload.hasField(pathTraverserConfig.viewIdFieldName)) {
                try {
                    // We are currently only supporting one ref, but with light change we could support a list of refs. Should we?
                    ObjectId objectId = payload.getReferenceFieldValue(pathTraverserConfig.viewIdFieldName);
                    if (objectId != null) {
                        alternateViewId = objectId.getId();
                    }
                    alternateViewId = writtenEvent.getWrittenInstance().getIdFieldValue(pathTraverserConfig.viewIdFieldName);
                } catch (Exception x) {
                    throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                            + pathTraverserConfig.viewIdFieldName
                            + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.", x);
                }
            } else {
                throw new IllegalStateException("instanceClassName:" + payload.getInstanceId().getClassName() + " requires that field:"
                        + pathTraverserConfig.viewIdFieldName
                        + " always be present and of type ObjectId. Please check that you have marked this field as mandatory.");
            }
        }
        return alternateViewId;
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

    public int getPathIndex() {
        return initialStepContext.getPathIndex();
    }

    public List<StepTraverser> getStepTraversers() {
        return stepTraversers;
    }

    @Override
    public String toString() {
        return "PathTraverser{"
                + "stepTraversers=" + stepTraversers
                + ", initialStepContext=" + initialStepContext
                + ", pathTraverserConfig=" + pathTraverserConfig + '}';
    }

}

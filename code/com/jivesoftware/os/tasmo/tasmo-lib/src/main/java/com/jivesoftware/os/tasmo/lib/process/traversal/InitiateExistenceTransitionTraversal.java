package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.ArrayListMultimap;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InitiateExistenceTransitionTraversal implements EventProcessor {

    private final Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> initialSteps;
    private final boolean idCentric;

    public InitiateExistenceTransitionTraversal(Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> initialSteps,
            boolean idCentric) {
        this.initialSteps = initialSteps;
        this.idCentric = idCentric;
    }

    @Override
    public void process(WrittenEventContext writtenEventContext, WrittenEvent writtenEvent) throws Exception {
        if (initialSteps == null || initialSteps.isEmpty()) {
            return;
        }

        for (ModelPathStepType stepType : ModelPathStepType.values()) {

            ArrayListMultimap<InitiateTraverserKey, PathTraverser> steps = initialSteps.get(stepType);
            if (steps != null) {

                TenantId tenantId = writtenEvent.getTenantId();
                Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
                final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
                long writtenOrderId = writtenEvent.getEventId();

                WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                ObjectId objectInstanceId = writtenInstance.getInstanceId();

                Set<PathTraverser> processedChains = new HashSet<>();

                for (InitiateTraverserKey key : steps.keySet()) {

                    for (PathTraverser step : steps.get(key)) {

                        if (!processedChains.contains(step)) {

                            ReferenceWithTimestamp ref = new ReferenceWithTimestamp(objectInstanceId,
                                    key.getTriggerFieldName(), writtenOrderId); // TODO Hmm
                            PathTraversalContext context = step.createContext(writtenEventContext, writtenEvent, writtenInstance.isDeletion());
                            context.setPathId(step.getPathIndex(), ref);
                            step.travers(writtenEvent, context, ref);
                            context.commit();

                            processedChains.add(step);
                        }
                    }
                }
            }
        }
    }
}

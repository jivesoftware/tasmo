package com.jivesoftware.os.tasmo.lib.process;

import com.google.common.collect.ArrayListMultimap;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ExistenceTransitionProcessor implements EventProcessor {

    private final Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, FieldProcessor>> initialSteps;
    private final boolean idCentric;

    public ExistenceTransitionProcessor(Map<ModelPathStepType, ArrayListMultimap<InitialStepKey, FieldProcessor>> initialSteps,
            boolean idCentric) {
        this.initialSteps = initialSteps;
        this.idCentric = idCentric;
    }


    @Override
    public boolean process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {
        boolean wasProcessed = false;
        if (initialSteps == null || initialSteps.isEmpty()) {
            return wasProcessed;
        }

        for (ModelPathStepType stepType : ModelPathStepType.values()) {

            ArrayListMultimap<InitialStepKey, FieldProcessor> steps = initialSteps.get(stepType);
            if (steps != null) {

                TenantId tenantId = writtenEvent.getTenantId();
                Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
                final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
                long writtenOrderId = writtenEvent.getEventId();

                WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                ObjectId objectInstanceId = writtenInstance.getInstanceId();
                ModifiedViewProvider modifiedViewProvider = batchContext.getModifiedViewProvider();

                Set<FieldProcessor> processedChains = new HashSet<>();
                Reference instanceReference = new Reference(objectInstanceId, writtenOrderId);

                for (InitialStepKey key : steps.keySet()) {

                    for (FieldProcessor step : steps.get(key)) {

                        if (!processedChains.contains(step)) {

                            ViewFieldContext context = step.createContext(modifiedViewProvider,
                                    writtenEvent, instanceReference, writtenInstance.isDeletion());
                            step.process(tenantIdAndCentricId, writtenEvent, context, instanceReference);
                            context.commit();

                            processedChains.add(step);

                            wasProcessed |= true;

                        }
                    }
                }
            }
        }

        return wasProcessed;
    }
}

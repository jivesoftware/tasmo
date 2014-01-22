/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.google.common.collect.ArrayListMultimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.Collection;

/**
 *
 */
public class RefRemovalProcessor implements EventProcessor {

    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitialStepKey, ExecutableStep> steps;
    private final boolean idCentric;

    public RefRemovalProcessor(ReferenceStore referenceStore, ArrayListMultimap<InitialStepKey, ExecutableStep> steps,
            boolean idCentric) {
        this.referenceStore = referenceStore;
        this.steps = steps;
        this.idCentric = idCentric;
    }

    @Override
    public boolean process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {

        boolean wasProcessed = false;
        if (steps == null || steps.isEmpty()) {
            return wasProcessed;
        }

        TenantId tenantId = writtenEvent.getTenantId();
        long writtenOrderId = writtenEvent.getEventId();
        ObjectId objectId = writtenEvent.getWrittenInstance().getInstanceId();
        WrittenInstance payload = writtenEvent.getWrittenInstance();
        Reference objectInstanceId = new Reference(objectId, writtenOrderId);
        ModifiedViewProvider modifiedViewProvider = batchContext.getModifiedViewProvider();

        for (InitialStepKey key : steps.keySet()) {
            String initialFieldName = key.getInitialFieldName();
            if (payload.hasField(key.getTriggerFieldName()) && (payload.hasField(initialFieldName) || payload.isDeletion())) {
                Collection<ExecutableStep> initialStepsForKey = steps.get(key);
                Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
                TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
                processRemoveRefs(tenantIdAndCentricId,
                    writtenOrderId, objectInstanceId, initialFieldName, initialStepsForKey, modifiedViewProvider, writtenEvent);

                wasProcessed |= true;
            }
        }
        return wasProcessed;
    }

    private void processRemoveRefs(
        final TenantIdAndCentricId tenantIdAndCentricId,
        final long removeAtTimestamp,
        final Reference objectInstanceId,
        final String initialFieldName,
        final Collection<ExecutableStep> initialSteps,
        final ModifiedViewProvider modifiedViewProvider,
        final WrittenEvent writtenEvent) throws Exception {

        referenceStore.remove_aId_aField(tenantIdAndCentricId, removeAtTimestamp, objectInstanceId, initialFieldName,
            new CallbackStream<Reference>() {
            @Override
            public Reference callback(Reference bId) throws Exception {
                if (bId != null && bId.getTimestamp() < removeAtTimestamp) {
                    for (ExecutableStep step : initialSteps) {
                        ViewFieldContext context = step.createContext(modifiedViewProvider, writtenEvent, objectInstanceId, true);
                        step.process(tenantIdAndCentricId, writtenEvent, context, bId);
                        context.commit();
                    }
                }
                return bId;
            }
        });
    }
}

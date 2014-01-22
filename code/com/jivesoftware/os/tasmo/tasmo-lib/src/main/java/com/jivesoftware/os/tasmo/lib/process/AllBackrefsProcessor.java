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
public class AllBackrefsProcessor implements EventProcessor {

    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitialStepKey, ExecutableStep> steps;
    private final boolean idCentric;

    public AllBackrefsProcessor(WrittenInstanceHelper writtenInstanceHelper, ReferenceStore referenceStore,
            ArrayListMultimap<InitialStepKey, ExecutableStep> steps, boolean idCentric) {
        this.writtenInstanceHelper = writtenInstanceHelper;
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
        final ObjectId objectId = writtenEvent.getWrittenInstance().getInstanceId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        Reference objectInstanceId = new Reference(objectId, writtenOrderId);
        Id centricId = idCentric ? writtenEvent.getCentricId() : Id.NULL;
        TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, centricId);

        ModifiedViewProvider modifiedViewProvider = batchContext.getModifiedViewProvider();

        for (final InitialStepKey key : steps.keySet()) {
            if (key.isDelete()) {
                continue;
            }
            if (writtenInstance.hasField(key.getTriggerFieldName())
                && writtenEvent.getWrittenInstance().hasField(key.getInitialFieldName())) {
                Collection<ExecutableStep> initialStepsForKey = steps.get(key);
                String initialFieldName = key.getInitialFieldName();

                Collection<Reference> bIds = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance,
                    key.getInitialFieldName(), writtenOrderId);
                processAddBackRefs(bIds, tenantIdAndCentricId, writtenOrderId, objectInstanceId, initialFieldName, initialStepsForKey,
                    modifiedViewProvider, writtenEvent);
                wasProcessed |= true;
            }
        }
        return wasProcessed;
    }

    private void processAddBackRefs(Collection<Reference> bIds,
        final TenantIdAndCentricId tenantIdAndCentricId,
        final long addAtTimestamp,
        final Reference objectInstanceId,
        final String initialFieldName,
        final Collection<ExecutableStep> initialSteps,
        final ModifiedViewProvider modifiedViewProvider,
        final WrittenEvent writtenEvent) throws Exception {

        if (bIds != null) {
            referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, addAtTimestamp, objectInstanceId, initialFieldName, bIds);
            for (final Reference bId : bIds) {
                for (final ExecutableStep step : initialSteps) {
                    final ViewFieldContext context = step.createContext(modifiedViewProvider, writtenEvent, bId, false);
                    step.process(tenantIdAndCentricId, writtenEvent, context, objectInstanceId);
                    context.commit();
                }
            }
        }
    }
}

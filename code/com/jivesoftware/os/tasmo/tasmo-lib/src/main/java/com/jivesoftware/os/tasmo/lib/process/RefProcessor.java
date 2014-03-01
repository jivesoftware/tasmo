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
 * @author jonathan.colt
 */
public class RefProcessor implements EventProcessor {

    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitialStepKey, FieldProcessor> steps;
    private final boolean idCentric;

    public RefProcessor(WrittenInstanceHelper writtenInstanceHelper, ReferenceStore referenceStore, ArrayListMultimap<InitialStepKey, FieldProcessor> steps,
            boolean idCentric) {
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
        ObjectId objectId = writtenEvent.getWrittenInstance().getInstanceId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        Reference objectInstanceId = new Reference(objectId, writtenOrderId);
        ModifiedViewProvider modifiedViewProvider = batchContext.getModifiedViewProvider();

        for (InitialStepKey key : steps.keySet()) {
            if (key.isDelete()) {
                continue;
            }
            if (writtenInstance.hasField(key.getTriggerFieldName())
                && writtenEvent.getWrittenInstance().hasField(key.getInitialFieldName())) {
                Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
                Collection<FieldProcessor> initialStepsForKey = steps.get(key);
                String initialFieldName = key.getInitialFieldName();
                TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);

                Collection<Reference> bIds = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, initialFieldName, writtenOrderId);
                if (bIds != null) {
                    link_Aid_to_Bids(tenantIdAndCentricId, writtenOrderId, objectInstanceId, initialFieldName, bIds, initialStepsForKey,
                        modifiedViewProvider, writtenEvent);
                }

                wasProcessed |= true;
            }
        }
        return wasProcessed;
    }

    private void link_Aid_to_Bids(final TenantIdAndCentricId tenantIdAndCentricId,
        long addAtTimestamp,
        final Reference objectInstanceId,
        final String initialFieldName,
        Collection<Reference> bIds,
        final Collection<FieldProcessor> initialSteps,
        final ModifiedViewProvider modifiedViewProvider,
        final WrittenEvent writtenEvent) throws Exception {
        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, addAtTimestamp, objectInstanceId, initialFieldName, bIds);
        for (FieldProcessor step : initialSteps) {
            ViewFieldContext context = step.createContext(modifiedViewProvider, writtenEvent, objectInstanceId, false);
            for (Reference bId : bIds) {
                step.process(tenantIdAndCentricId, writtenEvent, context, bId);
            }
            context.commit();
        }
    }
}

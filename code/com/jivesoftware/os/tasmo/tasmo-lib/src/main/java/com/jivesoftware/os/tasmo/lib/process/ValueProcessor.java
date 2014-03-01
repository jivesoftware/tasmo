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
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore.Transaction;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.HashSet;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class ValueProcessor implements EventProcessor {

    private final EventValueStore eventValueStore;
    private final ArrayListMultimap<InitialStepKey, FieldProcessor> steps;
    private final boolean idCentric;

    public ValueProcessor(EventValueStore eventValueStore, ArrayListMultimap<InitialStepKey, FieldProcessor> steps,
            boolean idCentric) {
        this.eventValueStore = eventValueStore;
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
        Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        long writtenOrderId = writtenEvent.getEventId();

        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        ObjectId objectInstanceId = writtenInstance.getInstanceId();
        ModifiedViewProvider modifiedViewProvider = batchContext.getModifiedViewProvider();

        if (writtenInstance.isDeletion()) {
            eventValueStore.removeObjectId(new TenantIdAndCentricId(tenantId, userId), writtenOrderId, objectInstanceId);
        } else {
            Transaction transaction = eventValueStore.begin(new TenantIdAndCentricId(tenantId, userId),
                writtenOrderId,
                writtenOrderId,
                objectInstanceId);
            for (InitialStepKey key : steps.keySet()) {
                if (writtenEvent.getWrittenInstance().hasField(key.getTriggerFieldName())) {
                    OpaqueFieldValue got = writtenInstance.getFieldValue(key.getTriggerFieldName());
                    if (got == null || got.isNull()) {
                        transaction.remove(key.getTriggerFieldName());
                    } else {
                        transaction.set(key.getTriggerFieldName(), got);
                    }
                }
            }
            eventValueStore.commit(transaction);
        }

        Set<FieldProcessor> processedChains = new HashSet<>();
        Reference instanceReference = new Reference(objectInstanceId, writtenOrderId);

        for (InitialStepKey key : steps.keySet()) {
            if (writtenEvent.getWrittenInstance().hasField(key.getTriggerFieldName())) {

                for (FieldProcessor step : steps.get(key)) {

                    if (!processedChains.contains(step)) {

                        ViewFieldContext context = step.createContext(modifiedViewProvider, writtenEvent, instanceReference, writtenInstance.isDeletion());
                        context.populateLeadNodeFields(eventValueStore, writtenEvent, objectInstanceId, step.getInitialFieldNames());
                        step.process(tenantIdAndCentricId, writtenEvent, context, instanceReference);
                        context.commit();

                        processedChains.add(step);

                        wasProcessed |= true;

                    }
                }
            }
        }

        return wasProcessed;
    }
}

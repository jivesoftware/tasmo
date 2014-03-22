/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.ArrayListMultimap;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore.Transaction;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class InitiateValueTraversal implements EventProcessor {

    private final EventValueStore eventValueStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> steps;
    private final boolean idCentric;

    public InitiateValueTraversal(EventValueStore eventValueStore, ArrayListMultimap<InitiateTraverserKey, PathTraverser> steps,
            boolean idCentric) {
        this.eventValueStore = eventValueStore;
        this.steps = steps;
        this.idCentric = idCentric;
    }

    @Override
    public void process(final WrittenEventContext writtenEventContext, WrittenEvent writtenEvent) throws Exception {
        if (steps == null || steps.isEmpty()) {
            return;
        }

        long timestamp = writtenEvent.getEventId();
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(writtenEvent.getTenantId(),
                (idCentric) ? writtenEvent.getCentricId() : Id.NULL);

        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        ObjectId instancId = writtenInstance.getInstanceId();

        if (writtenInstance.isDeletion()) {
            eventValueStore.removeObjectId(tenantIdAndCentricId, timestamp, instancId);
        } else {
            Transaction transaction = eventValueStore.begin(tenantIdAndCentricId,
                    timestamp,
                    timestamp,
                    instancId);
            for (InitiateTraverserKey key : steps.keySet()) {
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

        Set<PathTraverser> processedChains = new HashSet<>();

        for (InitiateTraverserKey key : steps.keySet()) {
            if (writtenEvent.getWrittenInstance().hasField(key.getTriggerFieldName())) {

                for (PathTraverser step : steps.get(key)) {

                    if (!processedChains.contains(step)) {

                        ReferenceWithTimestamp ref = new ReferenceWithTimestamp(instancId, key.getTriggerFieldName(), timestamp);
                        PathTraversalContext context = step.createContext(writtenEventContext, writtenEvent, writtenInstance.isDeletion());
                        context.setPathId(step.getPathIndex(), ref);
                        List<ReferenceWithTimestamp> versions = context.populateLeafNodeFields(eventValueStore, instancId, step.getInitialFieldNames());
                        context.addVersions(versions);
                        step.travers(writtenEvent, context, ref);
                        context.commit();

                        processedChains.add(step);
                    }
                }
            }
        }
    }
}

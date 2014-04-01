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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class InitiateValueTraversal implements EventProcessor {

    private final ConcurrencyChecker concurrencyChecker;
    private final EventValueStore eventValueStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers;

    public InitiateValueTraversal(ConcurrencyChecker concurrencyChecker,
            EventValueStore eventValueStore,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers) {
        this.concurrencyChecker = concurrencyChecker;
        this.eventValueStore = eventValueStore;
        this.traversers = traversers;
    }

    @Override
    public void process(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId, WrittenEvent writtenEvent, long threadTimestamp) throws Exception {
        if (traversers == null || traversers.isEmpty()) {
            return;
        }

        long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        ObjectId instanceId = writtenInstance.getInstanceId();

        for (InitiateTraverserKey key : traversers.keySet()) {
            if (writtenInstance.hasField(key.getTriggerFieldName())) {

                if (writtenInstance.isDeletion()) {

                    for (PathTraverser pathTraverser : traversers.get(key)) {

                        long highest = concurrencyChecker.highestVersion(tenantIdAndCentricId, instanceId, "*exists*", timestamp);
                        if (highest <= timestamp) {

                            PathTraversalContext context = pathTraverser.createContext(writtenEventContext,
                                    writtenEvent, threadTimestamp, true);
                            context.setPathId(pathTraverser.getPathIndex(), instanceId, timestamp);
                            List<ReferenceWithTimestamp> valueVersions = context.populateLeafNodeFields(tenantIdAndCentricId, eventValueStore,
                                    instanceId, pathTraverser.getInitialFieldNames());
                            context.addVersions(valueVersions);
                            pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(instanceId, timestamp));
                            context.commit(tenantIdAndCentricId, pathTraverser);
                        }
                    }
                } else {

                    for (PathTraverser pathTraverser : traversers.get(key)) {
                        PathTraversalContext context = pathTraverser.createContext(writtenEventContext,
                                writtenEvent, threadTimestamp, false);
                        List<ReferenceWithTimestamp> valueVersions = context.populateLeafNodeFields(tenantIdAndCentricId, eventValueStore,
                                instanceId, pathTraverser.getInitialFieldNames());

                        context.setPathId(pathTraverser.getPathIndex(), instanceId, timestamp);
                        context.addVersions(valueVersions);
                        pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(instanceId, timestamp));
                        context.commit(tenantIdAndCentricId, pathTraverser);

                        List<FieldVersion> want = new ArrayList<>();
                        for (ReferenceWithTimestamp valueVersion : valueVersions) {
                            want.add(new FieldVersion(instanceId, valueVersion.getFieldName(), valueVersion.getTimestamp()));
                        }
                        concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId, want);
                    }
                }
            }
        }
    }
}

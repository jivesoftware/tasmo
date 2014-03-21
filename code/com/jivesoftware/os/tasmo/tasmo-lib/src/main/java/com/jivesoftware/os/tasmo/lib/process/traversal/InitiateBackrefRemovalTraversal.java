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
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Collection;

/**
 *
 */
public class InitiateBackrefRemovalTraversal implements EventProcessor {

    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers;
    private final boolean idCentric;

    public InitiateBackrefRemovalTraversal(ReferenceStore referenceStore,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers,
            boolean idCentric) {
        this.referenceStore = referenceStore;
        this.traversers = traversers;
        this.idCentric = idCentric;
    }

    @Override
    public void process(final WrittenEventContext writtenEventContext, final WrittenEvent writtenEvent) throws Exception {
        if (traversers == null || traversers.isEmpty()) {
            return;
        }

        final long timestamp = writtenEvent.getEventId();
        TenantId tenantId = writtenEvent.getTenantId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();

        for (InitiateTraverserKey key : traversers.keySet()) {
            final String refFieldName = key.getRefFieldName();
            if (writtenInstance.hasField(key.getTriggerFieldName()) && (writtenInstance.hasField(refFieldName) || writtenInstance.isDeletion())) {
                Id userId = (idCentric) ? writtenEvent.getCentricId() : Id.NULL;
                final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
                final Collection<PathTraverser> pathTraversers = traversers.get(key);

                referenceStore.unlink(tenantIdAndCentricId, timestamp, instanceId, refFieldName,
                        new CallbackStream<ReferenceWithTimestamp>() {
                            @Override
                            public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                if (to != null && to.getTimestamp() < timestamp) {
                                    for (PathTraverser pathTraverser : pathTraversers) {

                                        PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, true);

                                        context.setPathId(pathTraverser.getPathIndex(), to);

                                        context.addVersion(new ReferenceWithTimestamp(
                                                        instanceId,
                                                        refFieldName,
                                                        to.getTimestamp()));

                                        pathTraverser.travers(writtenEvent, context, new ReferenceWithTimestamp(
                                                        instanceId,
                                                        refFieldName,
                                                        to.getTimestamp()));
                                        context.commit();
                                    }
                                }
                                return to;
                            }
                        });
            }
        }
    }
}

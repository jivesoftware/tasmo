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
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class InitiateBackRefTraversal implements EventProcessor {

    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers;
    private final boolean idCentric;

    public InitiateBackRefTraversal(WrittenInstanceHelper writtenInstanceHelper,
            ReferenceStore referenceStore,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers,
            boolean idCentric) {
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.referenceStore = referenceStore;
        this.traversers = traversers;
        this.idCentric = idCentric;
    }

    @Override
    public void process(final WrittenEventContext writtenEventContext, final WrittenEvent writtenEvent) throws Exception {
        if (traversers == null || traversers.isEmpty()) {
            return;
        }

        long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(writtenEvent.getTenantId(),
                idCentric ? writtenEvent.getCentricId() : Id.NULL);

        for (InitiateTraverserKey key : traversers.keySet()) {
            if (key.isDelete()) {
                continue;
            }
            if (writtenInstance.hasField(key.getTriggerFieldName()) && writtenInstance.hasField(key.getRefFieldName())) {

                final String refFieldName = key.getRefFieldName();

                Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, refFieldName);
                referenceStore.link(tenantIdAndCentricId, timestamp, instanceId, refFieldName, tos);

                final List<PathTraverser> pathTraversers = traversers.get(key);
                referenceStore.streamForwardRefs(tenantIdAndCentricId, instanceId.getClassName(), refFieldName, instanceId,
                        new CallbackStream<ReferenceWithTimestamp>() {

                            @Override
                            public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                if (to != null) {
                                    for (PathTraverser pathTraverser : pathTraversers) {

                                        PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, false);

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

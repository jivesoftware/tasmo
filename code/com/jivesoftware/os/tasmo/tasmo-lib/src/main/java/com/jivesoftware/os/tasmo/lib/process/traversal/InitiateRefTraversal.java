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
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore.FieldVersion;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class InitiateRefTraversal implements EventProcessor {

    private final ConcurrencyChecker concurrencyChecker;
    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers;
    private final boolean idCentric;

    public InitiateRefTraversal(ConcurrencyChecker concurrencyChecker,
            ReferenceStore referenceStore,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers,
            boolean idCentric) {
        this.concurrencyChecker = concurrencyChecker;
        this.referenceStore = referenceStore;
        this.forwardRefTraversers = forwardRefTraversers;
        this.backRefTraversers = backRefTraversers;
        this.idCentric = idCentric;
    }

    @Override
    public void process(final WrittenEventContext writtenEventContext, final WrittenEvent writtenEvent, final long threadTimestamp) throws Exception {

        TenantId tenantId = writtenEvent.getTenantId();
        final long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId,
                (idCentric) ? writtenEvent.getCentricId() : Id.NULL);

        Set<InitiateTraverserKey> allKeys = new HashSet();
        allKeys.addAll(forwardRefTraversers.keySet());
        allKeys.addAll(backRefTraversers.keySet());

        for (final InitiateTraverserKey key : allKeys) {
            if (writtenInstance.hasField(key.getTriggerFieldName())
                    && (writtenInstance.hasField(key.getRefFieldName()) || writtenInstance.isDeletion())) {

                final String refFieldName = key.getRefFieldName();

                long highest = concurrencyChecker.highestVersion(tenantIdAndCentricId.getTenantId(), instanceId, refFieldName, timestamp);

                referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), instanceId, refFieldName, threadTimestamp,
                        new CallbackStream<ReferenceWithTimestamp>() {
                            @Override
                            public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                if (to != null && to.getTimestamp() < timestamp) {
                                    travers(writtenEventContext, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, true);
                                }
                                return to;
                            }
                        });

                if (!writtenInstance.isDeletion()) {
                    concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId,
                            Arrays.asList(new FieldVersion(instanceId, refFieldName, highest)));

                    referenceStore.streamForwardRefs(tenantIdAndCentricId, instanceId.getClassName(), refFieldName, instanceId, threadTimestamp,
                            new CallbackStream<ReferenceWithTimestamp>() {

                                @Override
                                public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                    if (to != null) {
                                        travers(writtenEventContext, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, false);
                                    }
                                    return to;
                                }
                            });

                    concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId,
                            Arrays.asList(new FieldVersion(instanceId, refFieldName, highest)));
                }
            }
        }
    }

    private void travers(WrittenEventContext writtenEventContext,
            WrittenEvent writtenEvent,
            InitiateTraverserKey key,
            ObjectId instanceId,
            String refFieldName,
            ReferenceWithTimestamp to,
            long threadTimestamp,
            boolean removal) throws Exception {

        ReferenceWithTimestamp from = new ReferenceWithTimestamp(instanceId,
                refFieldName, to.getTimestamp());

        for (PathTraverser pathTraverser : forwardRefTraversers.get(key)) {

            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            context.setPathId(pathTraverser.getPathIndex(), from.getObjectId(), from.getTimestamp());
            context.addVersion(from);

            pathTraverser.travers(writtenEvent, context, new PathId(to.getObjectId(), to.getTimestamp()));
            context.commit(pathTraverser);
        }

        for (PathTraverser pathTraverser : backRefTraversers.get(key)) {

            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            context.setPathId(pathTraverser.getPathIndex(), to.getObjectId(), to.getTimestamp());
            context.addVersion(from);

            pathTraverser.travers(writtenEvent, context, new PathId(instanceId, to.getTimestamp()));
            context.commit(pathTraverser);
        }
    }

}

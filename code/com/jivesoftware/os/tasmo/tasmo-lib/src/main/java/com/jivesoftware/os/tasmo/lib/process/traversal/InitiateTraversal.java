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
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InitiateTraversal implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ConcurrencyChecker concurrencyChecker;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers;

    private final ReferenceStore referenceStore;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers;
    private final ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers;

    public InitiateTraversal(ConcurrencyChecker concurrencyChecker,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> traversers,
            ReferenceStore referenceStore,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers) {
        this.concurrencyChecker = concurrencyChecker;
        this.traversers = traversers;
        this.referenceStore = referenceStore;
        this.forwardRefTraversers = forwardRefTraversers;
        this.backRefTraversers = backRefTraversers;
    }

    @Override
    public void process(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {

        if (traversers != null && !traversers.isEmpty()) {
            LOG.startTimer("values");
            try {
                processValues(batchContext, tenantIdAndCentricId, writtenEvent, threadTimestamp);
            } finally {
                LOG.stopTimer("values");
            }
        }

        LOG.startTimer("refs");
        try {
            processRefs(batchContext, tenantIdAndCentricId, writtenEvent, threadTimestamp);
        } finally {
            LOG.stopTimer("refs");
        }

    }

    private void processValues(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId, WrittenEvent writtenEvent, long threadTimestamp) throws Exception {

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
                            List<ReferenceWithTimestamp> valueVersions = context.populateLeafNodeFields(tenantIdAndCentricId,
                                    instanceId, pathTraverser.getInitialFieldNames());
                            context.addVersions(pathTraverser.getPathIndex(), valueVersions);
                            pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(instanceId, timestamp));
                            writtenEventContext.paths++;
                            context.commit(tenantIdAndCentricId, pathTraverser);
                        }
                    }
                } else {

                    for (PathTraverser pathTraverser : traversers.get(key)) {
                        PathTraversalContext context = pathTraverser.createContext(writtenEventContext,
                                writtenEvent, threadTimestamp, false);
                        List<ReferenceWithTimestamp> valueVersions = context.populateLeafNodeFields(tenantIdAndCentricId,
                                instanceId, pathTraverser.getInitialFieldNames());

                        context.setPathId(pathTraverser.getPathIndex(), instanceId, timestamp);
                        context.addVersions(pathTraverser.getPathIndex(), valueVersions);
                        pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(instanceId, timestamp));
                        writtenEventContext.paths++;
                        context.commit(tenantIdAndCentricId, pathTraverser);

                        List<ConcurrencyStore.FieldVersion> want = new ArrayList<>();
                        for (ReferenceWithTimestamp valueVersion : valueVersions) {
                            want.add(new ConcurrencyStore.FieldVersion(instanceId, valueVersion.getFieldName(), valueVersion.getTimestamp()));
                        }
                        concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId, want);
                    }
                }
            }
        }
    }

    private void processRefs(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId, final WrittenEvent writtenEvent, final long threadTimestamp) throws Exception {

        final long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();

        Set<InitiateTraverserKey> allKeys = new HashSet();
        allKeys.addAll(forwardRefTraversers.keySet());
        allKeys.addAll(backRefTraversers.keySet());

        for (final InitiateTraverserKey key : allKeys) {
            if (writtenInstance.hasField(key.getTriggerFieldName())
                    && (writtenInstance.hasField(key.getRefFieldName()) || writtenInstance.isDeletion())) {

                final String refFieldName = key.getRefFieldName();

                long highest = concurrencyChecker.highestVersion(tenantIdAndCentricId, instanceId, refFieldName, timestamp);

                referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), instanceId, refFieldName, threadTimestamp,
                        new CallbackStream<ReferenceWithTimestamp>() {
                            @Override
                            public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                if (to != null && to.getTimestamp() < timestamp) {
                                    travers(writtenEventContext, tenantIdAndCentricId, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, true);
                                }
                                return to;
                            }
                        });

                if (!writtenInstance.isDeletion()) {
                    concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId,
                            Arrays.asList(new ConcurrencyStore.FieldVersion(instanceId, refFieldName, highest)));

                    referenceStore.streamForwardRefs(tenantIdAndCentricId, instanceId.getClassName(), refFieldName, instanceId, threadTimestamp,
                            new CallbackStream<ReferenceWithTimestamp>() {

                                @Override
                                public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                    if (to != null) {
                                        travers(writtenEventContext,
                                                tenantIdAndCentricId, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, false);
                                    }
                                    return to;
                                }
                            });

                    concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId,
                            Arrays.asList(new ConcurrencyStore.FieldVersion(instanceId, refFieldName, highest)));
                }
            }
        }
    }

    private void travers(WrittenEventContext writtenEventContext,
            TenantIdAndCentricId tenantIdAndCentricId,
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
            context.addVersion(pathTraverser.getPathIndex(), from);

            pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(to.getObjectId(), to.getTimestamp()));
            writtenEventContext.paths++;
            context.commit(tenantIdAndCentricId, pathTraverser);
        }

        for (PathTraverser pathTraverser : backRefTraversers.get(key)) {

            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            context.setPathId(pathTraverser.getPathIndex(), to.getObjectId(), to.getTimestamp());
            context.addVersion(pathTraverser.getPathIndex(), from);

            pathTraverser.travers(tenantIdAndCentricId, writtenEvent, context, new PathId(instanceId, to.getTimestamp()));
            writtenEventContext.paths++;
            context.commit(tenantIdAndCentricId, pathTraverser);
        }
    }

}

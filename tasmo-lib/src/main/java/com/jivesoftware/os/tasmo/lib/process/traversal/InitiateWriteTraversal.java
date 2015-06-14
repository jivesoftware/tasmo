/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.FieldVersion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class InitiateWriteTraversal implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ListMultimap<InitiateTraverserKey, PathTraverser> globalValueTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> userValueTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> globalForwardRefTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> userForwardRefTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> globalBackRefTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> userBackRefTraversers;

    public InitiateWriteTraversal(ListMultimap<InitiateTraverserKey, PathTraverser> globalValueTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> userValueTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> globalForwardRefTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> userForwardRefTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> globalBackRefTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> userBackRefTraversers) {
        this.globalValueTraversers = globalValueTraversers;
        this.userValueTraversers = userValueTraversers;
        this.globalForwardRefTraversers = globalForwardRefTraversers;
        this.userForwardRefTraversers = userForwardRefTraversers;
        this.globalBackRefTraversers = globalBackRefTraversers;
        this.userBackRefTraversers = userBackRefTraversers;
    }

    @Override
    public void process(WrittenEventContext batchContext,
            TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {

        processValues(globalValueTraversers, "values.global",
                batchContext, globalCentricId, globalCentricId, userCentricId, writtenEvent, threadTimestamp);
        processValues(userValueTraversers, "values.centric",
                batchContext, userCentricId, globalCentricId, userCentricId, writtenEvent, threadTimestamp);

        processRefs(globalForwardRefTraversers, globalBackRefTraversers, "refs.global",
                batchContext, globalCentricId, globalCentricId, userCentricId, writtenEvent, threadTimestamp);
        processRefs(userForwardRefTraversers, userBackRefTraversers, "refs.global",
                batchContext, userCentricId, globalCentricId, userCentricId, writtenEvent, threadTimestamp);

    }

    private void processValues(ListMultimap<InitiateTraverserKey, PathTraverser> valueTraversers,
            String timerName,
            WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {
        if (valueTraversers != null && !valueTraversers.isEmpty()) {
            LOG.startTimer(timerName);
            try {
                processValues(batchContext, tenantIdAndCentricId, globalCentricId, userCentricId, writtenEvent, valueTraversers, threadTimestamp);
            } finally {
                LOG.stopTimer(timerName);
            }
        }
    }

    private void processRefs(ListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers,
            String timerName,
            WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {
        LOG.startTimer(timerName);
        try {
            processRefs(batchContext,
                    tenantIdAndCentricId, globalCentricId, userCentricId, writtenEvent, forwardRefTraversers, backRefTraversers, threadTimestamp);
        } finally {
            LOG.stopTimer(timerName);
        }
    }

    private void processValues(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId,
            final TenantIdAndCentricId globalCentricId,
            final TenantIdAndCentricId userCentricId,
            final WrittenEvent writtenEvent,
            final ListMultimap<InitiateTraverserKey, PathTraverser> valueTraversers,
            final long threadTimestamp) throws Exception {
        final ConcurrencyChecker concurrencyChecker = writtenEventContext.getConcurrencyChecker();
        final long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();

        if (writtenInstance.isDeletion()) {
            long highest = concurrencyChecker.highestVersion(tenantIdAndCentricId, instanceId, "*exists*", timestamp);
            if (highest <= timestamp) {
                List<Callable<List<ViewField>>> callables = new ArrayList<>();
                for (InitiateTraverserKey key : valueTraversers.keySet()) {
                    if (writtenInstance.hasField(key.getTriggerFieldName()) || key.getRefFieldName() == null) { // TODO fix == null HACK!
                        for (final PathTraverser pathTraverser : valueTraversers.get(key)) {
                            callables.add(new Callable<List<ViewField>>() {

                                @Override
                                public List<ViewField> call() throws Exception {
                                    PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, true);
                                    PathContext pathContext = pathTraverser.createPathContext();
                                    LeafContext leafContext = new WriteLeafContext();
                                    pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), instanceId, timestamp);

                                    List<ReferenceWithTimestamp> valueVersions = leafContext.removeLeafNodeFields(writtenEventContext, pathContext);
                                    pathContext.addVersions(pathTraverser.getPathIndex(), valueVersions);

                                    pathTraverser.traverse(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext,
                                            new PathId(instanceId, timestamp));
                                    writtenEventContext.valuePaths++;
                                    List<ViewField> takeChanges = context.takeChanges(); // TODO add auto flush if writeableChanges is to large.
                                    writtenEventContext.changes += takeChanges.size();
                                    return takeChanges;
                                }
                            });
                        }
                    }
                }
                commit(writtenEventContext, tenantIdAndCentricId, callables);
            }

        } else {
            Set<String> fieldNames = new HashSet<>();
            for (InitiateTraverserKey key : valueTraversers.keySet()) {
                if (writtenInstance.hasField(key.getTriggerFieldName()) || key.getRefFieldName() == null) { // TODO fix == null HACK!
                    for (final PathTraverser pathTraverser : valueTraversers.get(key)) {
                        fieldNames.addAll(pathTraverser.getInitialFieldNames());
                    }
                }
            }

            String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = writtenEventContext
                    .getFieldValueReader()
                    .readFieldValues(tenantIdAndCentricId, instanceId, fieldNamesArray);

            final Map<String, ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> fieldValues = new HashMap<>();
            for (int i = 0; i < fieldNamesArray.length; i++) {
                fieldValues.put(fieldNamesArray[i], got[i]);
            }

            final Set<FieldVersion> want = new HashSet<>();
            List<Callable<List<ViewField>>> callables = new ArrayList<>();
            for (InitiateTraverserKey key : valueTraversers.keySet()) {
                if (writtenInstance.hasField(key.getTriggerFieldName()) || key.getRefFieldName() == null) { // TODO fix == null HACK!
                    for (final PathTraverser pathTraverser : valueTraversers.get(key)) {
                        callables.add(new Callable<List<ViewField>>() {

                            @Override
                            public List<ViewField> call() throws Exception {
                                PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, false);
                                PathContext pathContext = pathTraverser.createPathContext();
                                LeafContext leafContext = new WriteLeafContext();

                                Set<String> fieldNames = pathTraverser.getInitialFieldNames();
                                List<ReferenceWithTimestamp> valueVersions = leafContext.populateLeafNodeFields(tenantIdAndCentricId,
                                        writtenEventContext,
                                        pathContext,
                                        instanceId,
                                        fieldNames,
                                        fieldValues);

                                pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), instanceId, timestamp);
                                pathContext.addVersions(pathTraverser.getPathIndex(), valueVersions);
                                pathTraverser.traverse(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext,
                                        new PathId(instanceId, timestamp));
                                writtenEventContext.valuePaths++;
                                for (ReferenceWithTimestamp valueVersion : valueVersions) {
                                    want.add(new FieldVersion(valueVersion.getTenantIdAndCentricId(),
                                            instanceId, valueVersion.getFieldName(), valueVersion.getTimestamp()));
                                }
                                List<ViewField> takeChanges = context.takeChanges(); // TODO add auto flush if writeableChanges is to large.
                                writtenEventContext.changes += takeChanges.size();
                                return takeChanges;
                            }
                        });

                    }
                }
            }
            commit(writtenEventContext, tenantIdAndCentricId, callables);
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(want);
        }
    }

    private void processRefs(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId,
            final TenantIdAndCentricId globalCentricId,
            final TenantIdAndCentricId userCentricId,
            final WrittenEvent writtenEvent,
            final ListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            final ListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers,
            final long threadTimestamp) throws Exception {
        final ConcurrencyChecker concurrencyChecker = writtenEventContext.getConcurrencyChecker();
        final ReferenceStore referenceStore = writtenEventContext.getReferenceStore();
        final long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();

        Set<InitiateTraverserKey> allKeys = new HashSet<>();
        allKeys.addAll(forwardRefTraversers.keySet());
        allKeys.addAll(backRefTraversers.keySet());

        List<InitiateTraverserKey> keys = new ArrayList<>();
        List<String> accumulateRefFieldNames = new ArrayList<>();
        for (final InitiateTraverserKey key : allKeys) {
            if (writtenInstance.hasField(key.getTriggerFieldName())
                    && (writtenInstance.hasField(key.getRefFieldName()) || writtenInstance.isDeletion())) {
                keys.add(key);
                accumulateRefFieldNames.add(key.getRefFieldName());
            }
        }

        String[] refFieldNames = accumulateRefFieldNames.toArray(new String[accumulateRefFieldNames.size()]);
        List<Long> highestVersions = concurrencyChecker.highestVersions(tenantIdAndCentricId, instanceId, refFieldNames);

        List<Callable<List<ViewField>>> callables = new ArrayList<>();
        for (int i = 0; i < refFieldNames.length; i++) {
            final InitiateTraverserKey key = keys.get(i);
            final String refFieldName = refFieldNames[i];
            final long highest = highestVersions.get(i) == null ? timestamp : highestVersions.get(i);
            callables.add(new Callable<List<ViewField>>() {

                @Override
                public List<ViewField> call() throws Exception {
                    final List<ViewField> writeableChanges = new ArrayList<>();
                    referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), instanceId, refFieldName, threadTimestamp,
                            new CallbackStream<ReferenceWithTimestamp>() {
                                @Override
                                public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                    if (to != null && to.getTimestamp() < timestamp) {
                                        traverse(writtenEventContext,
                                                tenantIdAndCentricId,
                                                globalCentricId, userCentricId,
                                                writtenEvent,
                                                forwardRefTraversers,
                                                backRefTraversers,
                                                key, instanceId, refFieldName, to, threadTimestamp, true,
                                                writeableChanges);
                                    }
                                    return to;
                                }
                            });
                    return writeableChanges;
                }
            });
        }

        commit(writtenEventContext, tenantIdAndCentricId, callables);

        if (!writtenInstance.isDeletion()) {
            Set<FieldVersion> fieldVersions = new HashSet<>();
            for (int i = 0; i < refFieldNames.length; i++) {
                long highest = highestVersions.get(i) == null ? timestamp : highestVersions.get(i);
                fieldVersions.add(new FieldVersion(tenantIdAndCentricId, instanceId, refFieldNames[i], highest));
            }
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(fieldVersions);

            callables.clear();
            for (int i = 0; i < refFieldNames.length; i++) {
                final InitiateTraverserKey key = keys.get(i);
                final String refFieldName = refFieldNames[i];
                callables.add(new Callable<List<ViewField>>() {

                    @Override
                    public List<ViewField> call() throws Exception {
                        final List<ViewField> writeableChanges = new ArrayList<>();
                        referenceStore.streamForwardRefs(tenantIdAndCentricId,
                                Collections.singleton(instanceId.getClassName()),
                                refFieldName,
                                instanceId,
                                threadTimestamp,
                                new CallbackStream<ReferenceWithTimestamp>() {

                                    @Override
                                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                        if (to != null) {
                                            traverse(writtenEventContext,
                                                    tenantIdAndCentricId,
                                                    globalCentricId, userCentricId,
                                                    writtenEvent,
                                                    forwardRefTraversers,
                                                    backRefTraversers,
                                                    key, instanceId, refFieldName, to, threadTimestamp, false,
                                                    writeableChanges);
                                        }
                                        return to;
                                    }
                                });
                        return writeableChanges;
                    }
                });
            }
            commit(writtenEventContext, tenantIdAndCentricId, callables);
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(fieldVersions);
        }

    }

    private void traverse(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId,
            final TenantIdAndCentricId globalCentricId,
            final TenantIdAndCentricId userCentricId,
            final WrittenEvent writtenEvent,
            final ListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            final ListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers,
            InitiateTraverserKey key,
            final ObjectId instanceId,
            String refFieldName,
            final ReferenceWithTimestamp to,
            final long threadTimestamp,
            final boolean removal,
            List<ViewField> writeableChanges) throws Exception {

        final ReferenceWithTimestamp from = new ReferenceWithTimestamp(tenantIdAndCentricId, instanceId, refFieldName, to.getTimestamp());

        for (final PathTraverser pathTraverser : forwardRefTraversers.get(key)) {
            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            PathContext pathContext = pathTraverser.createPathContext();
            LeafContext leafContext = new WriteLeafContext();
            pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), from.getObjectId(), from.getTimestamp());
            pathContext.addVersions(pathTraverser.getPathIndex(), Arrays.asList(from));

            pathTraverser.traverse(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext,
                    new PathId(to.getObjectId(), to.getTimestamp()));
            writtenEventContext.refPaths++;
            List<ViewField> takeChanges = context.takeChanges();
            writtenEventContext.changes += takeChanges.size();
            writeableChanges.addAll(takeChanges); // TODO add auto flush if writeableChanges is getting to be to large.
        }

        for (final PathTraverser pathTraverser : backRefTraversers.get(key)) {
            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            PathContext pathContext = pathTraverser.createPathContext();
            LeafContext leafContext = new WriteLeafContext();
            pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), to.getObjectId(), to.getTimestamp());
            pathContext.addVersions(pathTraverser.getPathIndex(), Arrays.asList(from));

            pathTraverser.traverse(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext,
                    new PathId(instanceId, to.getTimestamp()));
            writtenEventContext.backRefPaths++;
            List<ViewField> takeChanges = context.takeChanges();
            writtenEventContext.changes += takeChanges.size();
            writeableChanges.addAll(takeChanges); // TODO add auto flush if writeableChanges is getting to be to large.
        }

    }

    private void commit(WrittenEventContext writtenEventContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            List<Callable<List<ViewField>>> callables) throws Exception {

        List<ViewField> writeableChanges = new ArrayList<>();
        for (Callable<List<ViewField>> changes : callables) {
            writeableChanges.addAll(changes.call());
        }
        writtenEventContext.getCommitChange().commitChange(writtenEventContext, tenantIdAndCentricId, writeableChanges);
    }

}

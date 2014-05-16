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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class InitiateTraversal implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ConcurrencyChecker concurrencyChecker;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> valueTraversers;

    private final ReferenceStore referenceStore;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers;
    private final ListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers;
    private final ListeningExecutorService executorService;

    public InitiateTraversal(ListeningExecutorService executorService,
            ConcurrencyChecker concurrencyChecker,
            ListMultimap<InitiateTraverserKey, PathTraverser> traversers,
            ReferenceStore referenceStore,
            ListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers,
            ListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers) {
        this.executorService = executorService;
        this.concurrencyChecker = concurrencyChecker;
        this.valueTraversers = traversers;
        this.referenceStore = referenceStore;
        this.forwardRefTraversers = forwardRefTraversers;
        this.backRefTraversers = backRefTraversers;
    }

    @Override
    public void process(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {

        if (valueTraversers != null && !valueTraversers.isEmpty()) {
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
            final TenantIdAndCentricId tenantIdAndCentricId,
            final WrittenEvent writtenEvent,
            final long threadTimestamp) throws Exception {

        final long timestamp = writtenEvent.getEventId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        final ObjectId instanceId = writtenInstance.getInstanceId();

        if (writtenInstance.isDeletion()) {
            long highest = concurrencyChecker.highestVersion(tenantIdAndCentricId, instanceId, "*exists*", timestamp);
            if (highest <= timestamp) {
                List<Callable<List<ViewFieldChange>>> callables = new ArrayList<>();
                for (InitiateTraverserKey key : valueTraversers.keySet()) {
                    if (writtenInstance.hasField(key.getTriggerFieldName()) || key.getRefFieldName() == null) { // TODO fix == null HACK!
                        for (final PathTraverser pathTraverser : valueTraversers.get(key)) {
                            callables.add(new Callable<List<ViewFieldChange>>() {

                                @Override
                                public List<ViewFieldChange> call() throws Exception {
                                    PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, true);
                                    PathContext pathContext = pathTraverser.createPathContext();
                                    LeafContext leafContext = new LeafContext(writtenEventContext);
                                    pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), instanceId, timestamp);

                                    List<ReferenceWithTimestamp> valueVersions = leafContext.removeLeafNodeFields(pathContext);
                                    pathContext.addVersions(pathTraverser.getPathIndex(), valueVersions);

                                    pathTraverser.traverse(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext,
                                            new PathId(instanceId, timestamp));
                                    writtenEventContext.valuePaths++;
                                    List<ViewFieldChange> takeChanges = context.takeChanges(); // TODO add auto flush if writeableChanges is to large.
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

            final List<ConcurrencyStore.FieldVersion> want = new ArrayList<>();
            List<Callable<List<ViewFieldChange>>> callables = new ArrayList<>();
            for (InitiateTraverserKey key : valueTraversers.keySet()) {
                if (writtenInstance.hasField(key.getTriggerFieldName()) || key.getRefFieldName() == null) { // TODO fix == null HACK!
                    for (final PathTraverser pathTraverser : valueTraversers.get(key)) {
                        callables.add(new Callable<List<ViewFieldChange>>() {

                            @Override
                            public List<ViewFieldChange> call() throws Exception {
                                PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, false);
                                PathContext pathContext = pathTraverser.createPathContext();
                                LeafContext leafContext = new LeafContext(writtenEventContext);

                                Set<String> fieldNames = pathTraverser.getInitialFieldNames();
                                List<ReferenceWithTimestamp> valueVersions = leafContext.populateLeafNodeFields(tenantIdAndCentricId, pathContext,
                                        instanceId, fieldNames, fieldValues);

                                pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), instanceId, timestamp);
                                pathContext.addVersions(pathTraverser.getPathIndex(), valueVersions);
                                pathTraverser.traverse(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext,
                                        new PathId(instanceId, timestamp));
                                writtenEventContext.valuePaths++;
                                for (ReferenceWithTimestamp valueVersion : valueVersions) {
                                    want.add(new ConcurrencyStore.FieldVersion(instanceId, valueVersion.getFieldName(), valueVersion.getTimestamp()));
                                }
                                List<ViewFieldChange> takeChanges = context.takeChanges(); // TODO add auto flush if writeableChanges is to large.
                                writtenEventContext.changes += takeChanges.size();
                                return takeChanges;
                            }
                        });

                    }
                }
            }
            commit(writtenEventContext, tenantIdAndCentricId, callables);
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId, want);
        }
    }

    private void processRefs(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId, final WrittenEvent writtenEvent, final long threadTimestamp) throws Exception {

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

        List<Callable<List<ViewFieldChange>>> callables = new ArrayList<>();
        for (int i = 0; i < refFieldNames.length; i++) {
            final InitiateTraverserKey key = keys.get(i);
            final String refFieldName = refFieldNames[i];
            final long highest = highestVersions.get(i) == null ? timestamp : highestVersions.get(i);
            callables.add(new Callable<List<ViewFieldChange>>() {

                @Override
                public List<ViewFieldChange> call() throws Exception {
                    final List<ViewFieldChange> writeableChanges = new ArrayList<>();
                    referenceStore.unlink(tenantIdAndCentricId, Math.max(timestamp, highest), instanceId, refFieldName, threadTimestamp,
                            new CallbackStream<ReferenceWithTimestamp>() {
                                @Override
                                public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                                    if (to != null && to.getTimestamp() < timestamp) {
                                        traverse(writtenEventContext,
                                                tenantIdAndCentricId, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, true,
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
            List<ConcurrencyStore.FieldVersion> fieldVersions = new ArrayList<>();
            for (int i = 0; i < refFieldNames.length; i++) {
                long highest = highestVersions.get(i) == null ? timestamp : highestVersions.get(i);
                fieldVersions.add(new ConcurrencyStore.FieldVersion(instanceId, refFieldNames[i], highest));
            }
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId, fieldVersions);

            callables.clear();
            for (int i = 0; i < refFieldNames.length; i++) {
                final InitiateTraverserKey key = keys.get(i);
                final String refFieldName = refFieldNames[i];
                callables.add(new Callable<List<ViewFieldChange>>() {

                    @Override
                    public List<ViewFieldChange> call() throws Exception {
                        final List<ViewFieldChange> writeableChanges = new ArrayList<>();
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
                                                    tenantIdAndCentricId, writtenEvent, key, instanceId, refFieldName, to, threadTimestamp, false,
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
            concurrencyChecker.checkIfModifiedOutFromUnderneathMe(tenantIdAndCentricId, fieldVersions);
        }

    }

    private void traverse(final WrittenEventContext writtenEventContext,
            final TenantIdAndCentricId tenantIdAndCentricId,
            final WrittenEvent writtenEvent,
            InitiateTraverserKey key,
            final ObjectId instanceId,
            String refFieldName,
            final ReferenceWithTimestamp to,
            final long threadTimestamp,
            final boolean removal,
            List<ViewFieldChange> writeableChanges) throws Exception {

        final ReferenceWithTimestamp from = new ReferenceWithTimestamp(instanceId,
                refFieldName, to.getTimestamp());

        for (final PathTraverser pathTraverser : forwardRefTraversers.get(key)) {
            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            PathContext pathContext = pathTraverser.createPathContext();
            LeafContext leafContext = new LeafContext(writtenEventContext);
            pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), from.getObjectId(), from.getTimestamp());
            pathContext.addVersions(pathTraverser.getPathIndex(), Arrays.asList(from));

            pathTraverser.traverse(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext,
                    new PathId(to.getObjectId(), to.getTimestamp()));
            writtenEventContext.refPaths++;
            List<ViewFieldChange> takeChanges = context.takeChanges();
            writtenEventContext.changes += takeChanges.size();
            writeableChanges.addAll(takeChanges); // TODO add auto flush if writeableChanges is getting to be to large.
        }

        for (final PathTraverser pathTraverser : backRefTraversers.get(key)) {
            PathTraversalContext context = pathTraverser.createContext(writtenEventContext, writtenEvent, threadTimestamp, removal);
            PathContext pathContext = pathTraverser.createPathContext();
            LeafContext leafContext = new LeafContext(writtenEventContext);
            pathContext.setPathId(writtenEventContext, pathTraverser.getPathIndex(), to.getObjectId(), to.getTimestamp());
            pathContext.addVersions(pathTraverser.getPathIndex(), Arrays.asList(from));

            pathTraverser.traverse(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext,
                    new PathId(instanceId, to.getTimestamp()));
            writtenEventContext.backRefPaths++;
            List<ViewFieldChange> takeChanges = context.takeChanges();
            writtenEventContext.changes += takeChanges.size();
            writeableChanges.addAll(takeChanges); // TODO add auto flush if writeableChanges is getting to be to large.
        }

    }

    private void commit(WrittenEventContext writtenEventContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            List<Callable<List<ViewFieldChange>>> callables) throws Exception {

        List<ViewFieldChange> writeableChanges = new ArrayList<>();
        boolean async = false;
        if (async) {
            List<ListenableFuture<List<ViewFieldChange>>> futures = new ArrayList<>();
            for (Callable<List<ViewFieldChange>> callable : callables) {
                futures.add(executorService.submit(callable));
            }
            ListenableFuture<List<List<ViewFieldChange>>> allAsList = Futures.allAsList(futures);
            for (List<ViewFieldChange> changes : allAsList.get()) {
                writeableChanges.addAll(changes);
            }
        } else {
            for (Callable<List<ViewFieldChange>> changes : callables) {
                writeableChanges.addAll(changes.call());
            }
        }
        writtenEventContext.getCommitChange().commitChange(writtenEventContext, tenantIdAndCentricId, writeableChanges);
    }

}

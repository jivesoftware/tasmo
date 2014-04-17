/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange.ViewFieldChangeType;
import com.jivesoftware.os.tasmo.model.process.LeafNodeFields;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Context used while processing an event for a specific model path. Passed to ChainableProcessSteps which implement the operations performed at each step in
 * the path.
 */
public class PathTraversalContext {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WrittenEvent writtenEvent;
    private final WrittenEventProvider writtenEventProvider;
    private final CommitChange commitChange;
    private final Id alternateViewId;
    private final long threadTimestamp;
    private final PathId[] modelPathIdState;
    private final List<ReferenceWithTimestamp>[] modelPathVersionState;
    private final boolean removalContext;
    private LeafNodeFields leafNodeFields; // uck
    private int addIsRemovingFields = 0; // uck
    private final List<ViewFieldChange> changes = new ArrayList<>(); // uck

    public PathTraversalContext(WrittenEvent writtenEvent,
            WrittenEventProvider writtenEventProvider,
            CommitChange commitChange,
            Id alternateViewId,
            int numberOfPathIds,
            long threadTimestamp,
            boolean removalContext) {
        this.writtenEvent = writtenEvent;
        this.writtenEventProvider = writtenEventProvider;
        this.commitChange = commitChange;
        this.alternateViewId = alternateViewId;
        this.threadTimestamp = threadTimestamp;
        this.modelPathIdState = new PathId[numberOfPathIds];
        this.modelPathVersionState = new List[numberOfPathIds];
        for (int i = 0; i < numberOfPathIds; i++) {
            modelPathVersionState[i] = new ArrayList<>();
        }
        this.removalContext = removalContext;
    }

    public long getThreadTimestamp() {
        return threadTimestamp;
    }

    public void setPathId(int pathIndex, ObjectId id, long timestamp) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("!!!!!!!!---------- SetPath:" + pathIndex + " |||| " + id + " @ " + timestamp + " " + " remove:" + removalContext);
        }
        this.modelPathIdState[pathIndex] = new PathId(id, timestamp);
    }

    public void addVersion(int pathIndex, ReferenceWithTimestamp version) {
        cleanUpVersions(pathIndex);
        this.modelPathVersionState[pathIndex].add(version);
    }

    public void addVersions(int pathIndex, Collection<ReferenceWithTimestamp> versions) {
        cleanUpVersions(pathIndex);
        this.modelPathVersionState[pathIndex].addAll(versions);
    }

    private int lastVersionPathIndex = 0;

    private void cleanUpVersions(int pathIndex) {
        int i = pathIndex;
        if (pathIndex > lastVersionPathIndex) {
            i++;
        }
        lastVersionPathIndex = pathIndex;
        for (; i < modelPathVersionState.length; i++) {
            modelPathVersionState[i].clear();
        }
    }

    private List<ReferenceWithTimestamp> copyOfVersions() {
        List<ReferenceWithTimestamp> copy = new ArrayList<>();
        for (List<ReferenceWithTimestamp> versiosns : modelPathVersionState) {
            copy.addAll(versiosns);
        }
        return copy;
    }

    public PathId getPathId(int pathIndex) {
        return this.modelPathIdState[pathIndex];
    }

    @Override
    public String toString() {
        return "ViewFieldContext{"
                + "modelPathIdState=" + Arrays.toString(modelPathIdState)
                + "modelPathVersionState=" + modelPathVersionState
                + ", rawFieldValue=" + leafNodeFields
                + '}';
    }

    public List<ReferenceWithTimestamp> populateLeafNodeFields(TenantIdAndCentricId tenantIdAndCentricId,
            EventValueStore eventValueStore,
            ObjectId objectInstanceId, List<String> fieldNames) {

        LeafNodeFields fieldsToPopulate = writtenEventProvider.createLeafNodeFields();
        long latestTimestamp = writtenEvent.getEventId();
        List<ReferenceWithTimestamp> versions = new ArrayList<>();
        if (!removalContext) {
            addIsRemovingFields = 0;
            WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
            for (String fieldName : fieldNames) {
                if (writtenInstance.hasField(fieldName)) {
                    OpaqueFieldValue fieldValue = writtenInstance.getFieldValue(fieldName);
                    if (fieldValue.isNull()) {
                        addIsRemovingFields++;
                    }
                }
            }

            String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = eventValueStore.get(tenantIdAndCentricId,
                    objectInstanceId, fieldNamesArray);

            if (got != null) {
                for (ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> g : got) {
                    if (g != null) {
                        String fieldName = g.getColumn();
                        OpaqueFieldValue fieldValue = g.getValue();
                        long timestamp = g.getTimestamp();
                        latestTimestamp = Math.max(latestTimestamp, timestamp);
                        if (fieldValue == null || fieldValue.isNull()) {
                            fieldsToPopulate.removeField(fieldName);
                        } else {
                            fieldsToPopulate.addField(fieldName, fieldValue);
                        }
                        versions.add(new ReferenceWithTimestamp(objectInstanceId, fieldName, timestamp));
                    }
                }
            }
        }

        this.leafNodeFields = fieldsToPopulate;
        return versions;
    }

    public void writeViewFields(String viewClassName, String modelPathId, ObjectId objectInstanceId) throws IOException {
        if (leafNodeFields == null) {
            return;
        }
        if (!removalContext && !leafNodeFields.hasFields() && addIsRemovingFields == 0) {
            return;
        }
        ObjectId objectId = objectInstanceId;
        Id viewId = alternateViewId;
        if (viewId == null) {
            viewId = objectId.getId();
        }

        ViewFieldChange update = new ViewFieldChange(writtenEvent.getEventId(),
                writtenEvent.getActorId(),
                (removalContext) ? ViewFieldChangeType.remove : ViewFieldChangeType.add, // uck
                new ObjectId(viewClassName, viewId),
                modelPathId,
                Arrays.copyOf(modelPathIdState, modelPathIdState.length),
                copyOfVersions(),
                leafNodeFields.toStringForm(),
                threadTimestamp);
        changes.add(update);
    }

    public void commit(TenantIdAndCentricId tenantIdAndCentricId, PathTraverser traverser) throws Exception { // TODO this method doesn't belong in this class
        try {
            if (!changes.isEmpty()) {
                if (LOG.isTraceEnabled()) {
                    int i = 1;
                    for (ViewFieldChange change : changes) {
                        LOG.trace("!!!!!!!!----------" + i + "." + ((removalContext) ? "REMOVABLE" : "ADDABLE")
                                + " PATH:" + Arrays.toString(change.getModelPathInstanceIds())
                                + " versions:" + modelPathIdStateAsString(change.getModelPathVersions(), true)
                                + " v=" + change.getValue() + " t=" + change.getTimestamp() + " traverse: [");
                        for (String t : pathTraverserAsString(traverser)) {
                            LOG.trace(t);
                        }
                    }
                }

                commitChange.commitChange(tenantIdAndCentricId, changes);

            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("!!!!!!!!---------- DIDN'T " + ((removalContext) ? "REMOVE" : "ADD")
                            + " INCOMPLETE PATH:" + Arrays.toString(modelPathIdState)
                            + " versions:" + modelPathIdStateAsString(copyOfVersions(), true) + " traverse: [");
                    for (String t : pathTraverserAsString(traverser)) {
                        LOG.trace(t);
                    }
                }
            }
        } finally {
            changes.clear();
            modelPathVersionState[lastVersionPathIndex].clear();
        }
    }

    List<String> pathTraverserAsString(PathTraverser pathTraverser) {
        List<String> lines = new ArrayList<>();
        for (StepTraverser stepTraverser : pathTraverser.getStepTraversers()) {
            lines.add("!!!!!!!!----------  " + stepTraverser + ", ");
        }
        lines.add("!!!!!!!!----------  ]");
        return lines;
    }

    String modelPathIdStateAsString(List<ReferenceWithTimestamp> path, boolean fields) {
        String s = "[";
        for (ReferenceWithTimestamp p : path) {
            if (p == null) {
                s += "null, ";
            } else {
                s += p.getObjectId().getClassName() + "." + p.getObjectId().getId().toStringForm();
                if (fields) {
                    s += "." + p.getFieldName();
                }
                s += "." + p.getTimestamp() + ", ";
            }
        }
        return s + ']';
    }
}

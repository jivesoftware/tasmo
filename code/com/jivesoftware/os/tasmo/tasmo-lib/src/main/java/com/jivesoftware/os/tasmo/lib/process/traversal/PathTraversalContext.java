/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
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

    private final WrittenEvent writtenEvent;
    final TenantIdAndCentricId tenantIdAndCentricId;
    private final WrittenEventProvider writtenEventProvider;
    private final CommitChange commitChange;
    private final Id alternateViewId;
    private final ReferenceWithTimestamp[] modelPathIdState;
    private final List<ReferenceWithTimestamp> modelPathVersionState;
    private long leafNodeTimestamp;
    private final boolean removalContext;
    private LeafNodeFields leafNodeFields; // uck
    private int addIsRemovingFields = 0; // uck
    private final List<ViewFieldChange> changes = new ArrayList<>(); // uck

    public PathTraversalContext(WrittenEvent writtenEvent,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventProvider writtenEventProvider,
            CommitChange commitChange,
            Id alternateViewId,
            int numberOfPathIds,
            boolean removalContext) {
        this.writtenEvent = writtenEvent;
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.writtenEventProvider = writtenEventProvider;
        this.commitChange = commitChange;
        this.alternateViewId = alternateViewId;
        this.modelPathIdState = new ReferenceWithTimestamp[numberOfPathIds];
        this.modelPathVersionState = new ArrayList<>();
        this.removalContext = removalContext;

        // A.a -> B
    }

    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return tenantIdAndCentricId;
    }

    public void setPathId(int pathIndex, ReferenceWithTimestamp id) {
        //System.out.println(Debug.caller(10) + " |||| SetPath:" + pathIndex + " |||| " + versionObjectId);
        this.modelPathIdState[pathIndex] = id;
    }

    public void addVersion(ReferenceWithTimestamp version) {
        //System.out.println(Debug.caller(10) + " |||| SetPath:" + pathIndex + " |||| " + versionObjectId);
        this.modelPathVersionState.add(version);
    }
    public void addVersions(Collection<ReferenceWithTimestamp> versions) {
        //System.out.println(Debug.caller(10) + " |||| SetPath:" + pathIndex + " |||| " + versionObjectId);
        this.modelPathVersionState.addAll(versions);
    }

    public ReferenceWithTimestamp getPathId(int pathIndex) {
        return this.modelPathIdState[pathIndex];
    }

    @Override
    public String toString() {
        return "ViewFieldContext{"
                + "modelPathIdState=" + modelPathIdState
                + "modelPathVersionState=" + modelPathVersionState
                + ", rawFieldValue=" + leafNodeFields
                + '}';
    }

    public List<ReferenceWithTimestamp> populateLeafNodeFields(EventValueStore eventValueStore, ObjectId objectInstanceId, List<String> fieldNames) {
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
        this.leafNodeTimestamp = latestTimestamp;
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
                0, // Deprecated
                0, // Deprecated
                tenantIdAndCentricId,
                writtenEvent.getActorId(),
                (removalContext) ? ViewFieldChangeType.remove : ViewFieldChangeType.add, // uck
                new ObjectId(viewClassName, viewId),
                modelPathId,
                Arrays.copyOf(modelPathIdState, modelPathIdState.length),
                new ArrayList<>(modelPathVersionState),
                leafNodeFields.toStringForm(),
                getHighWaterTimestamp());
        changes.add(update);
    }

    private long getHighWaterTimestamp() {
        long highwaterTimestamp = leafNodeTimestamp;
        for (ReferenceWithTimestamp reference : modelPathIdState) {
            highwaterTimestamp = Math.max(highwaterTimestamp, reference.getTimestamp());
        }
        return highwaterTimestamp;
    }

    public void commit() throws Exception { // TODO this method doesn't belong in this class
        System.out.println("---------- Anything to write?:"+!changes.isEmpty() +" pathIds:"+Arrays.deepToString(modelPathIdState)+" versions:"+modelPathVersionState);

        if (!changes.isEmpty()) {
            System.out.println("Wrote:"+changes);
            commitChange.commitChange(tenantIdAndCentricId, changes);
            changes.clear();
            modelPathVersionState.clear();
        }
    }
}

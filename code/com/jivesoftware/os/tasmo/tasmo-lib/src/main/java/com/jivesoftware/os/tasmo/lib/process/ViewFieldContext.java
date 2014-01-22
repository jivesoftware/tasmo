/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

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
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Context used while processing an event for a specific model path. Passed to ChainableProcessSteps which implement the operations performed at each step in
 * the path.
 */
public class ViewFieldContext {

    final long eventId;
    final TenantIdAndCentricId tenantIdAndCentricId;
    final Id actorId;
    private final WrittenEventProvider writtenEventProvider;
    private final CommitChange commitChange;
    private final long contextTimestamp;
    private final Id alternateViewId;
    private final Reference[] modelPathInstanceState;
    private long leafNodeTimestamp;
    private final boolean removalContext;
    private LeafNodeFields leafNodeFields; // uck
    private int addIsRemovingFields = 0; // uck
    private final List<ViewFieldChange> changes = new ArrayList<>(); // uck

    public ViewFieldContext(long eventId,
        TenantIdAndCentricId tenantIdAndCentricId,
        Id actorId,
        WrittenEventProvider writtenEventProvider,
        CommitChange commitChange,
        long contextTimestamp,
        Id alternateViewId,
        int numberOfPathIds,
        boolean removalContext) {
        this.eventId = eventId;
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.actorId = actorId;
        this.writtenEventProvider = writtenEventProvider;
        this.commitChange = commitChange;
        this.contextTimestamp = contextTimestamp;
        this.alternateViewId = alternateViewId;
        this.modelPathInstanceState = new Reference[numberOfPathIds];
        this.removalContext = removalContext;
    }

    public void setPathId(int pathIndex, Reference versionObjectId) {
        this.modelPathInstanceState[pathIndex] = versionObjectId;
    }

    public Reference getPathId(int pathIndex) {
        return this.modelPathInstanceState[pathIndex];
    }

    @Override
    public String toString() {
        return "ViewFieldContext{"
            + "viewFieldKey=" + modelPathInstanceState
            + ", rawFieldValue=" + leafNodeFields
            + '}';
    }

    void populateLeadNodeFields(EventValueStore eventValueStore, WrittenEvent writtenEvent, ObjectId objectInstanceId, List<String> fieldNames) {
        LeafNodeFields fieldsToPopulate = writtenEventProvider.createLeafNodeFields();
        long latestTimestamp = contextTimestamp;

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
                    }
                }
            }
        }

        this.leafNodeFields = fieldsToPopulate;
        this.leafNodeTimestamp = latestTimestamp;
    }

    void writeViewFields(String viewClassName, String modelPathId, ObjectId objectInstanceId) throws IOException {
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

        ViewFieldChange update = new ViewFieldChange(eventId,
                0, // Deprecated
                0, // Deprecated
                tenantIdAndCentricId,
                actorId,
                (removalContext) ? ViewFieldChangeType.remove : ViewFieldChangeType.add, // uck
                new ObjectId(viewClassName, viewId),
                modelPathId,
                copyModelPathObjectIds(),
                leafNodeFields.toStringForm(),
                getHighWaterTimestamp());
        changes.add(update);
    }

    private ObjectId[] copyModelPathObjectIds() {
        ObjectId[] ids = new ObjectId[modelPathInstanceState.length];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = modelPathInstanceState[i].getObjectId();
        }
        return ids;
    }

    private long getHighWaterTimestamp() {
        long highwaterTimestamp = leafNodeTimestamp;
        for (Reference reference : modelPathInstanceState) {
            highwaterTimestamp = Math.max(highwaterTimestamp, reference.getTimestamp());
        }
        return highwaterTimestamp;
    }

    public void commit() throws Exception { // TODO this method doesn't belong in this class

        if (!changes.isEmpty()) {
            commitChange.commitChange(tenantIdAndCentricId, changes);
            changes.clear();
        }
    }
}

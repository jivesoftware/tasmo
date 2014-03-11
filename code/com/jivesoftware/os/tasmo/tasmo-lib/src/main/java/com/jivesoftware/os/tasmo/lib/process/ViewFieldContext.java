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
import com.jivesoftware.os.tasmo.model.process.LeafNodeFields;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Context used while processing an event for a specific model path. Passed to ChainableProcessSteps which implement the operations performed at each step in
 * the path.
 */
public class ViewFieldContext {

    final TenantIdAndCentricId tenantIdAndCentricId;
    final Id actorId;
    private final WrittenEventProvider writtenEventProvider;
    private final CommitChange commitChange;
    private final Reference[] modelPathInstanceState;
    private LeafNodeFields leafNodeFields; // uck
    private final List<ViewFieldChange> changes = new ArrayList<>(); // uck

    public ViewFieldContext(
        TenantIdAndCentricId tenantIdAndCentricId,
        Id actorId,
        WrittenEventProvider writtenEventProvider,
        CommitChange commitChange,
        int numberOfPathIds) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.actorId = actorId;
        this.writtenEventProvider = writtenEventProvider;
        this.commitChange = commitChange;
        this.modelPathInstanceState = new Reference[numberOfPathIds];
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

    void populateLeadNodeFields(EventValueStore eventValueStore, ObjectId objectInstanceId, List<String> fieldNames) {
        LeafNodeFields fieldsToPopulate = writtenEventProvider.createLeafNodeFields();

        String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = eventValueStore.get(tenantIdAndCentricId,
            objectInstanceId, fieldNamesArray);

        if (got != null) {
            for (ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> g : got) {
                if (g != null) {
                    String fieldName = g.getColumn();
                    OpaqueFieldValue fieldValue = g.getValue();

                    if (fieldValue != null && !(fieldValue.isNull())) {
                        fieldsToPopulate.addField(fieldName, fieldValue);
                    }
                }
            }
        }

        this.leafNodeFields = fieldsToPopulate;
    }

    void writeViewFields(String viewClassName, String modelPathId, ObjectId objectInstanceId) throws IOException {
        if (leafNodeFields == null) {
            return;
        }
        ObjectId objectId = objectInstanceId;

        Id viewId = objectId.getId();

        ViewFieldChange update = new ViewFieldChange(0, //Deprecated
            0, // Deprecated
            0, // Deprecated
            tenantIdAndCentricId,
            actorId,
            null, // Deprecated
            new ObjectId(viewClassName, viewId),
            modelPathId,
            copyModelPathObjectIds(),
            leafNodeFields.toStringForm(),
            0); //Deprecated
        changes.add(update);
    }

    private ObjectId[] copyModelPathObjectIds() {
        ObjectId[] ids = new ObjectId[modelPathInstanceState.length];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = modelPathInstanceState[i].getObjectId();
        }
        return ids;
    }

    public void commit() throws Exception { // TODO this method doesn't belong in this class

        if (!changes.isEmpty()) {
            commitChange.commitChange(tenantIdAndCentricId, changes);
            changes.clear();
        }
    }
}

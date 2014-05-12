package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.LeafNodeFields;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class LeafContext {

    private final WrittenEventContext writtenEventContext;
    private final boolean removalContext;

    private LeafNodeFields leafNodeFields; // uck
    private int addIsRemovingFields = 0; // uck

    public LeafContext(WrittenEventContext writtenEventContext, boolean removalContext) {
        this.writtenEventContext = writtenEventContext;
        this.removalContext = removalContext;
    }

    public byte[] toBytes() throws IOException {
        if (leafNodeFields == null) {
            return null;
        }
        if (!removalContext && !leafNodeFields.hasFields() && addIsRemovingFields == 0) {
            return null;
        }
        return leafNodeFields.toBytes();
    }

    public List<ReferenceWithTimestamp> removeLeafNodeFields(PathContext pathContext) {
        WrittenEvent writtenEvent = writtenEventContext.getEvent();
        long latestTimestamp = writtenEvent.getEventId();
        LeafNodeFields fieldsToPopulate = writtenEventContext.getWrittenEventProvider().createLeafNodeFields();
        List<ReferenceWithTimestamp> versions = new ArrayList<>();
        pathContext.setLastTimestamp(latestTimestamp);
        this.leafNodeFields = fieldsToPopulate;
        return versions;
    }

    public List<ReferenceWithTimestamp> populateLeafNodeFields(TenantIdAndCentricId tenantIdAndCentricId,
        PathContext pathContext,
        ObjectId objectInstanceId,
        List<String> fieldNames) {

        WrittenEvent writtenEvent = writtenEventContext.getEvent();
        long latestTimestamp = writtenEvent.getEventId();
        LeafNodeFields fieldsToPopulate = writtenEventContext.getWrittenEventProvider().createLeafNodeFields();
        List<ReferenceWithTimestamp> versions = new ArrayList<>();
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
        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = writtenEventContext.getFieldValueReader().readFieldValues(tenantIdAndCentricId,
            objectInstanceId, fieldNamesArray);
        writtenEventContext.readLeaves++;
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
        pathContext.setLastTimestamp(latestTimestamp);
        this.leafNodeFields = fieldsToPopulate;
        return versions;
    }
}

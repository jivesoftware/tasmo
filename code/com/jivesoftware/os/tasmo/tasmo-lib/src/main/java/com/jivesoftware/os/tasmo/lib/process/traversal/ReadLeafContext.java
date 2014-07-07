package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.LeafNodeFields;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class ReadLeafContext implements LeafContext {

    private LeafNodeFields leafNodeFields; // uck

    @Override
    public byte[] toBytes() throws IOException {
        if (leafNodeFields == null) {
            return null;
        }
        return leafNodeFields.toBytes();
    }

    @Override
    public List<ReferenceWithTimestamp> populateLeafNodeFields(TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEventContext writtenEventContext,
        PathContext pathContext,
        ObjectId objectInstanceId,
        Set<String> fieldNames,
        Map<String, ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> fieldValues) {

        LeafNodeFields fieldsToPopulate = writtenEventContext.getWrittenEventProvider().createLeafNodeFields();
        List<ReferenceWithTimestamp> versions = new ArrayList<>();

        String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);

        for (String fieldName : fieldNamesArray) {
            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> g = fieldValues.get(fieldName);
            if (g != null) {
                OpaqueFieldValue fieldValue = g.getValue();
                long timestamp = g.getTimestamp();
                pathContext.setLastTimestamp(timestamp); // ??
                fieldsToPopulate.addField(fieldName, fieldValue);
                versions.add(new ReferenceWithTimestamp(objectInstanceId, fieldName, timestamp));
            }
        }
        this.leafNodeFields = fieldsToPopulate;
        return versions;
    }

    @Override
    public List<ReferenceWithTimestamp> removeLeafNodeFields(WrittenEventContext writtenEventContext, PathContext pathContext) {
        return null;
    }

}

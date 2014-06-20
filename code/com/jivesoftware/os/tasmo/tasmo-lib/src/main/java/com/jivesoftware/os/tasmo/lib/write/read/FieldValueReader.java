package com.jivesoftware.os.tasmo.lib.write.read;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;

/**
 *
 * @author jonathan
 */
public interface FieldValueReader {

    ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId objectInstanceId,
            String[] fieldNamesArray);
}

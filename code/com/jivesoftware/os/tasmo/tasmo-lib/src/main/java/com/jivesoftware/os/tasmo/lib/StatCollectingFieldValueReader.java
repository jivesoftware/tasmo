package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;

public class StatCollectingFieldValueReader implements FieldValueReader {

    private final TasmoProcessingStats tasmoProcessingStats;
    private final FieldValueReader delegateFieldValueReader;

    public StatCollectingFieldValueReader(TasmoProcessingStats tasmoProcessingStats, FieldValueReader delegateFieldValueReader) {
        this.tasmoProcessingStats = tasmoProcessingStats;
        this.delegateFieldValueReader = delegateFieldValueReader;
    }

    @Override
    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues(TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId objectInstanceId,
        String[] fieldNamesArray) {
        long start = System.currentTimeMillis();
        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues = delegateFieldValueReader
            .readFieldValues(tenantIdAndCentricId, objectInstanceId, fieldNamesArray);

        String key = "fieldsFrom:" + objectInstanceId.getClassName();
        tasmoProcessingStats.latency("READ FIELDS", key, System.currentTimeMillis() - start);
        return readFieldValues;
    }
}

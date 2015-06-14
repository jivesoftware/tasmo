package com.jivesoftware.os.tasmo.lib.read;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;

/**
 *
 * @author jonathan
 */
public class EventValueStoreFieldValueReader implements FieldValueReader {

    private final EventValueStore eventValueStore;

    public EventValueStoreFieldValueReader(EventValueStore eventValueStore) {
        this.eventValueStore = eventValueStore;
    }

    @Override
    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId objectInstanceId,
            String[] fieldNamesArray) {
        return eventValueStore.get(tenantIdAndCentricId, objectInstanceId, fieldNamesArray);
    }
}

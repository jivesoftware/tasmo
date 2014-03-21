package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Used to support Multiversion concurrency control
 */
public class ConcurrencyStore {

    private final RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> updatedStore;
    private final RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> deletedStore;

    public ConcurrencyStore(RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> updatedStore,
            RowColumnValueStore<TenantId, ObjectId, String, Long, RuntimeException> deletedStore) {
        this.updatedStore = updatedStore;
        this.deletedStore = deletedStore;
    }

    public void deleted(TenantId tenant, ObjectId objectId, String[] fields, long timestamp) {
        //System.out.println("|||| Deleted:" + objectId + " fields:" + Arrays.deepToString(fields) + " t=" + timestamp + " " + Debug.caller(4));
        updatedStore.multiRemove(tenant, objectId, fields, new ConstantTimestamper(timestamp));
    }

    public void updated(TenantId tenant, ObjectId objectId, String[] fields, long timestamp) {
        Long[] values = new Long[fields.length];
        Arrays.fill(values, timestamp);
        //System.out.println("|||| Update:" + objectId + " fields:" + Arrays.deepToString(fields) + " t=" + timestamp + " " + Debug.caller(4));
        updatedStore.multiAdd(tenant, objectId, fields, values, null, new ConstantTimestamper(timestamp));
    }

    public long highest(TenantId tenant, ObjectId objectId, String field, long defaultTimestamp) {
        Long got = updatedStore.get(tenant, objectId, field, null, null);
        if (got == null) {
            return defaultTimestamp;
        }
        return got;
    }

    /**
     *
     * @param tenantId
     * @param expected
     * @return expected instance if no instance was modified.
     */
    public List<FieldVersion> checkIfModified(TenantId tenantId, List<FieldVersion> expected) {
        // TODO add multi get support.
        List<FieldVersion> was = new ArrayList<>(expected.size());
        for (FieldVersion e : expected) {
            if (e == null) { // TODO resolve: got == null should be impossible
                was.add(e);
            } else {
                Long got = updatedStore.get(tenantId, e.objectId, e.fieldName, null, null);
                if (got == null) { // TODO resolve: got == null should be impossible
                    was.add(e);
                } else if (got != e.version) {
                    was.add(new FieldVersion(e.objectId, e.fieldName, got));
                    return was; // Means epected has been modified
                } else {
                    was.add(e);
                }
            }
        }
        return expected;
    }

    public static class FieldVersion {

        private final ObjectId objectId;
        private final String fieldName;
        private final Long version;

        public FieldVersion(ObjectId objectId, String fieldName, Long version) {
            if (objectId == null) {
                throw new IllegalArgumentException("objectId cannot be null");
            }
            if (fieldName == null) {
                throw new IllegalArgumentException("fieldName cannot be null");
            }

            this.objectId = objectId;
            this.fieldName = fieldName;
            this.version = version;
        }

        public ObjectId getObjectId() {
            return objectId;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Long getVersion() {
            return version;
        }

        @Override
        public String toString() { // Hacked for human readability when in a list.
            return objectId + "." + fieldName + "=" + version;
        }

    }
}

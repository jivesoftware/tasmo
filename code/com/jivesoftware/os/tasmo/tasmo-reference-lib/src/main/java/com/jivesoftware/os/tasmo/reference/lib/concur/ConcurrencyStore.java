package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Used to support Multiversion concurrency control
 */
public class ConcurrencyStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final String EXISTS = "*exists*";

    private final RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updatedStore;

    public ConcurrencyStore(RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updatedStore) {
        this.updatedStore = updatedStore;
    }

    public void updated(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields, long timestamp) {
        Long[] values = new Long[fields.length];
        Arrays.fill(values, timestamp);
        updatedStore.multiAdd(tenantIdAndCentricId, objectId, fields, values, null, new ConstantTimestamper(timestamp));
    }

    public void addObjectId(List<ExistenceUpdate> existenceUpdates) {

        List<TenantRowColumValueTimestampAdd<TenantIdAndCentricId, ObjectId, String, Long>> batch = new ArrayList<>();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            batch.add(new TenantRowColumValueTimestampAdd<>(existenceUpdate.tenantId,
                    existenceUpdate.objectId, EXISTS,
                    existenceUpdate.timestamp,
                    new ConstantTimestamper(existenceUpdate.timestamp)));
            if (LOG.isTraceEnabled()) {

                LOG.trace("Object EXISTS:{} time:{}", new Object[]{existenceUpdate.objectId, existenceUpdate.timestamp});
            }
        }
        updatedStore.multiRowsMultiAdd(batch);
    }

    public void removeObjectId(List<ExistenceUpdate> existenceUpdates) {
        List<TenantRowColumnTimestampRemove<TenantIdAndCentricId, ObjectId, String>> batch = new ArrayList<>();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            batch.add(new TenantRowColumnTimestampRemove<>(existenceUpdate.tenantId,
                    existenceUpdate.objectId, EXISTS,
                    new ConstantTimestamper(existenceUpdate.timestamp)));
            if (LOG.isTraceEnabled()) {

                LOG.trace("Object REMOVED:{} time:{}", new Object[]{existenceUpdate.objectId, existenceUpdate.timestamp});
            }
        }
        updatedStore.multiRowsMultiRemove(batch);
    }

    public Set<ObjectId> getExistence(TenantIdAndCentricId tenantId, Set<ObjectId> objectIds) {
        List<ObjectId> orderObjectIds = new ArrayList<>(objectIds);
        List<Long> multiRowGet = updatedStore.multiRowGet(tenantId, orderObjectIds, EXISTS, null, null);
        Set<ObjectId> existence = new HashSet<>();
        for (int i = 0; i < orderObjectIds.size(); i++) {
            if (multiRowGet.get(i) != null) {
                existence.add(orderObjectIds.get(i));
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Check existence {} TRUE", new Object[]{orderObjectIds.get(i)});
                }
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Check existence {} FALSE", new Object[]{orderObjectIds.get(i)});
                }
            }
        }
        return existence;
    }

    public long highest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String field, long defaultTimestamp) {
        Long got = updatedStore.get(tenantIdAndCentricId, objectId, field, null, null);
        if (got == null) {
            return defaultTimestamp;
        }
        return got;
    }

    public List<Long> highests(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields) {
        return updatedStore.multiGet(tenantIdAndCentricId, objectId, fields, null, null);
    }

    /**
     *
     * @param tenantId
     * @param expected
     * @return expected instance if no instance was modified.
     */
    public Set<FieldVersion> checkIfModified(TenantIdAndCentricId tenantId, Set<FieldVersion> expectedSet) {
        List<FieldVersion> expected = new ArrayList<>(expectedSet);
        List<ObjectId> rows = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        for (FieldVersion e : expected) {
            rows.add(e.getObjectId());
            columns.add(e.getFieldName());
        }
        if (expected.size() > 100) {
            System.out.println("expected:" + expected.size() + " vs " + new HashSet<>(expected).size());
        }

        List<Map<String, Long>> results = updatedStore.multiRowMultiGet(tenantId, rows, columns, null, null);
        Set<FieldVersion> was = new HashSet<>(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            FieldVersion e = expected.get(i);
            Map<String, Long> result = results.get(i);
            if (result == null) {
                was.add(e);
            } else {
                Long got = result.get(e.fieldName);
                if (got == null) {
                    was.add(e);
                } else if (!Objects.equals(got, e.version)) {
                    was.add(new FieldVersion(e.objectId, e.fieldName, got));
                    return was; // Means epected has been modified
                } else {
                    was.add(e);
                }
            }
        }
        return expectedSet;

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

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 37 * hash + Objects.hashCode(this.objectId);
            hash = 37 * hash + Objects.hashCode(this.fieldName);
            hash = 37 * hash + Objects.hashCode(this.version);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final FieldVersion other = (FieldVersion) obj;
            if (!Objects.equals(this.objectId, other.objectId)) {
                return false;
            }
            if (!Objects.equals(this.fieldName, other.fieldName)) {
                return false;
            }
            if (!Objects.equals(this.version, other.version)) {
                return false;
            }
            return true;
        }
    }
}

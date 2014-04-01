package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
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
            LOG.trace("Object EXISTS:{} time:{}", new Object[]{existenceUpdate.objectId, existenceUpdate.timestamp});
        }
        updatedStore.multiRowsMultiAdd(batch);
    }

    public void removeObjectId(List<ExistenceUpdate> existenceUpdates) {
        List<TenantRowColumnTimestampRemove<TenantIdAndCentricId, ObjectId, String>> batch = new ArrayList<>();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            batch.add(new TenantRowColumnTimestampRemove<>(existenceUpdate.tenantId,
                    existenceUpdate.objectId, EXISTS,
                    new ConstantTimestamper(existenceUpdate.timestamp)));
            LOG.trace("Object REMOVED:{} time:{}", new Object[]{existenceUpdate.objectId, existenceUpdate.timestamp});
        }
        updatedStore.multiRowsMultiRemove(batch);
    }

    public Set<ObjectId> getExistence(List<ExistenceUpdate> existenceUpdates) {

        ListMultimap<TenantIdAndCentricId, ObjectId> tenantIdsObjectIds = ArrayListMultimap.create();
        for (ExistenceUpdate existenceUpdate : existenceUpdates) {
            tenantIdsObjectIds.put(existenceUpdate.tenantId, existenceUpdate.objectId);
        }

        Set<ObjectId> existence = new HashSet<>();
        for (TenantIdAndCentricId tenantId : tenantIdsObjectIds.keySet()) {
            List<ObjectId> orderObjectIds = tenantIdsObjectIds.get(tenantId);
            List<Long> multiRowGet = updatedStore.multiRowGet(tenantId, orderObjectIds, EXISTS, null, null);
            for (int i = 0; i < orderObjectIds.size(); i++) {
                if (multiRowGet.get(i) != null) {
                    existence.add(orderObjectIds.get(i));
                    LOG.trace("Check existence {} TRUE", new Object[]{orderObjectIds.get(i)});
                } else {
                    LOG.trace("Check existence {} FALSE", new Object[]{orderObjectIds.get(i)});
                }
            }
        }
        return existence;
    }

    public Set<ObjectId> getExistence(TenantIdAndCentricId tenantId, Set<ObjectId> objectIds) {
        List<ObjectId> orderObjectIds = new ArrayList<>(objectIds);
        List<Long> multiRowGet = updatedStore.multiRowGet(tenantId, orderObjectIds, EXISTS, null, null);
        Set<ObjectId> existence = new HashSet<>();
        for (int i = 0; i < orderObjectIds.size(); i++) {
            if (multiRowGet.get(i) != null) {
                existence.add(orderObjectIds.get(i));
                LOG.trace("Check existence {} TRUE", new Object[]{orderObjectIds.get(i)});
            } else {
                LOG.trace("Check existence {} FALSE", new Object[]{orderObjectIds.get(i)});
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

    /**
     *
     * @param tenantId
     * @param expected
     * @return expected instance if no instance was modified.
     */
    public List<FieldVersion> checkIfModified(TenantIdAndCentricId tenantId, List<FieldVersion> expected) {
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

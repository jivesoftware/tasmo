/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib.concur;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class HBaseBackedConcurrencyStore implements ConcurrencyStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final String EXISTS = "*exists*";

    private final RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updatedStore;

    public HBaseBackedConcurrencyStore(RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, Long, RuntimeException> updatedStore) {
        this.updatedStore = updatedStore;
    }

    @Override
    public void updated(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields, long timestamp) {
        Long[] values = new Long[fields.length];
        Arrays.fill(values, timestamp);
        updatedStore.multiAdd(tenantIdAndCentricId, objectId, fields, values, null, new ConstantTimestamper(timestamp));
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public long highest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String field, long defaultTimestamp) {
        Long got = updatedStore.get(tenantIdAndCentricId, objectId, field, null, null);
        if (got == null) {
            return defaultTimestamp;
        }
        return got;
    }

    @Override
    public List<Long> highests(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fields) {
        return updatedStore.multiGet(tenantIdAndCentricId, objectId, fields, null, null);
    }

    /**
     *
     * @param expectedSet
     * @return expectedSet instance if no instance was modified.
     */
    @Override
    public Set<FieldVersion> checkIfModified(Set<FieldVersion> expectedSet) {
        ListMultimap<TenantIdAndCentricId, FieldVersion> perTenantFieldVersions = ArrayListMultimap.create();
        for (FieldVersion fieldVersion : expectedSet) {
            perTenantFieldVersions.put(fieldVersion.getTenantIdAndCentricId(), fieldVersion);
        }
        Set<FieldVersion> unexpected = new HashSet<>();
        for (TenantIdAndCentricId tenantIdAndCentricId : perTenantFieldVersions.keySet()) {
            List<FieldVersion> expected = perTenantFieldVersions.get(tenantIdAndCentricId);
            List<FieldVersion> was = checkIfModified(tenantIdAndCentricId, expected);
            if (was != expected) {
                unexpected.addAll(was);
            }
        }
        if (unexpected.isEmpty()) {
            return expectedSet;
        }
        return unexpected;
    }

    private List<FieldVersion> checkIfModified(TenantIdAndCentricId tenantId, List<FieldVersion> expectedSet) {
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
        List<FieldVersion> was = new ArrayList<>(expected.size());
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
                    was.add(new FieldVersion(tenantId, e.objectId, e.fieldName, got));
                    return was; // Means epected has been modified
                } else {
                    was.add(e);
                }
            }
        }
        return expectedSet;
    }
}

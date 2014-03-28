/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.events;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class EventValueStore {

    private final RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventValueStore;
    private final ConcurrencyStore concurrencyStore;

    public EventValueStore(ConcurrencyStore concurrencyStore,
            RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> classFieldValueStore,
            EventValueCacheProvider cacheProvider) {
        this.concurrencyStore = concurrencyStore;
        this.eventValueStore = classFieldValueStore;
    }

    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] get(
            TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fieldNames) {
        return eventValueStore.multiGetEntries(tenantIdAndCentricId, objectId, fieldNames, null, null);
    }

    public void removeObjectId(TenantIdAndCentricId tenantIdAndCentricId,
            long removeAtTimestamp,
            ObjectId objectId,
            String[] fieldNames) {
        String[] fields = Arrays.copyOf(fieldNames, fieldNames.length + 1);
        fields[fields.length - 1] = "deleted";
        concurrencyStore.updated(tenantIdAndCentricId, objectId, fields, removeAtTimestamp - 1);
        if (fieldNames.length > 0) {
            eventValueStore.multiRemove(tenantIdAndCentricId, objectId, fieldNames, new ConstantTimestamper(removeAtTimestamp + 1));
        }
        concurrencyStore.updated(tenantIdAndCentricId, objectId, fields, removeAtTimestamp);
    }

    public Transaction begin(TenantIdAndCentricId tenantIdAndCentricId,
            long addAtTimestamp,
            long removeAtTimestamp,
            ObjectId objectId) {
        return new Transaction(tenantIdAndCentricId, addAtTimestamp, removeAtTimestamp, objectId);
    }

    public void commit(Transaction transaction) {

        ObjectId objectInstanceId = transaction.objectInstanceId;
        if (!transaction.addedFieldNames.isEmpty()) {
            String[] takeAddedFieldNames = transaction.takeAddedFieldNames();
            OpaqueFieldValue[] takeAddedValues = transaction.takeAddedValues();

            String[] fields = Arrays.copyOf(takeAddedFieldNames, takeAddedFieldNames.length + 1);
            fields[fields.length - 1] = "deleted";

            concurrencyStore.updated(transaction.tenantIdAndCentricId, objectInstanceId, fields, transaction.addAtTimestamp - 1);

            eventValueStore.multiAdd(
                    transaction.tenantIdAndCentricId,
                    objectInstanceId,
                    takeAddedFieldNames,
                    takeAddedValues,
                    null, new ConstantTimestamper(transaction.addAtTimestamp));

            concurrencyStore.updated(transaction.tenantIdAndCentricId, objectInstanceId, fields, transaction.addAtTimestamp);

        }
        if (!transaction.removedFieldNames.isEmpty()) {
            String[] takeRemovedFieldNames = transaction.takeRemovedFieldNames();

            String[] fields = Arrays.copyOf(takeRemovedFieldNames, takeRemovedFieldNames.length + 1);
            fields[fields.length - 1] = "deleted";
            concurrencyStore.updated(transaction.tenantIdAndCentricId, objectInstanceId, fields, transaction.removeAtTimestamp - 1);

            eventValueStore.multiRemove(
                    transaction.tenantIdAndCentricId,
                    objectInstanceId,
                    takeRemovedFieldNames,
                    new ConstantTimestamper(transaction.removeAtTimestamp));

            concurrencyStore.updated(transaction.tenantIdAndCentricId, objectInstanceId, fields, transaction.removeAtTimestamp);

        }
    }

    final public static class Transaction {

        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final long addAtTimestamp;
        private final long removeAtTimestamp;
        private final ObjectId objectInstanceId;
        private List<String> addedFieldNames;
        private List<OpaqueFieldValue> addedValues;
        private List<String> removedFieldNames;
        private final Thread constructingThread;

        private Transaction(TenantIdAndCentricId tenantIdAndCentricId,
                long addAtTimestamp,
                long removeAtTimestamp,
                ObjectId objectId) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.addAtTimestamp = addAtTimestamp;
            this.removeAtTimestamp = removeAtTimestamp;
            this.objectInstanceId = objectId;
            this.addedFieldNames = Lists.newLinkedList();
            this.addedValues = Lists.newLinkedList();
            this.removedFieldNames = Lists.newLinkedList();
            this.constructingThread = Thread.currentThread();
        }

        public int size() {
            return addedFieldNames.size() + removedFieldNames.size();
        }

        public void set(String fieldName, OpaqueFieldValue value) {
            if (!Thread.currentThread().equals(constructingThread)) {
                throw new IllegalStateException("calling thread must be the same as creating thread.");
            }
            addedFieldNames.add(fieldName);
            addedValues.add(value);
        }

        public String[] takeAddedFieldNames() {
            try {
                return addedFieldNames.toArray(new String[addedFieldNames.size()]);
            } finally {
                addedFieldNames = Lists.newArrayList();
            }
        }

        public OpaqueFieldValue[] takeAddedValues() {
            try {
                return addedValues.toArray(new OpaqueFieldValue[addedValues.size()]);
            } finally {
                addedValues = Lists.newArrayList();
            }
        }

        public void remove(String fieldName) {
            if (!Thread.currentThread().equals(constructingThread)) {
                throw new IllegalStateException("calling thread must be the same as creating thread.");
            }
            removedFieldNames.add(fieldName);
        }

        public String[] takeRemovedFieldNames() {
            try {
                return removedFieldNames.toArray(new String[removedFieldNames.size()]);
            } finally {
                removedFieldNames = Lists.newArrayList();
            }
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 23 * hash + (this.tenantIdAndCentricId != null ? this.tenantIdAndCentricId.hashCode() : 0);
            hash = 23 * hash + (this.objectInstanceId != null ? this.objectInstanceId.hashCode() : 0);
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
            final Transaction other = (Transaction) obj;
            if ((this.tenantIdAndCentricId == null) ? (other.tenantIdAndCentricId != null) : !this.tenantIdAndCentricId.equals(other.tenantIdAndCentricId)) {
                return false;
            }
            if (this.objectInstanceId != other.objectInstanceId && (this.objectInstanceId == null || !this.objectInstanceId.equals(other.objectInstanceId))) {
                return false;
            }
            return true;
        }
    }
}

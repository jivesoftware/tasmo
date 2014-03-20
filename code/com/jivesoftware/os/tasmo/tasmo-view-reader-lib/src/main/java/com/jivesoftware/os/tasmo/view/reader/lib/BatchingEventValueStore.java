/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class BatchingEventValueStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> eventValueStore;
    private final ThreadLocal<GetBatch> getBatch = new ThreadLocal<GetBatch>() {
        @Override
        protected GetBatch initialValue() {
            return new GetBatch();
        }
    };

    public BatchingEventValueStore(RowColumnValueStore<TenantIdAndCentricId, ObjectId, String, OpaqueFieldValue, RuntimeException> classFieldValueStore) {
        this.eventValueStore = classFieldValueStore;
    }

    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] get(
        TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fieldNames) {
        return eventValueStore.multiGetEntries(tenantIdAndCentricId, objectId, fieldNames, null, null);
    }

    public void addRequest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fieldNames,
        CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> callbackStream) {
        getBatch.get().addRequest(tenantIdAndCentricId, objectId, fieldNames, callbackStream);
    }

    public boolean executeBatch() throws Exception {
        ListMultimap<GetRequest, CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>> requests = getBatch.get().drain();
        if (requests.isEmpty()) {
            return false;
        }
        List<KeyedColumnValueCallbackStream<ObjectId, String, OpaqueFieldValue, Long>> callbacks = new ArrayList<>(requests.size());
        TenantIdAndCentricId tenantIdAndCentricId = null;
        for (Map.Entry<GetRequest, Collection<CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>>> entry : requests.asMap().entrySet()) {
            final GetRequest request = entry.getKey();
            final Collection<CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>> requestCallbacks = entry.getValue();
            tenantIdAndCentricId = request.tenantIdAndCentricId;
            callbacks.add(new KeyedColumnValueCallbackStream<>(request.objectId, new CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>() {
                final Set<String> requestedFields = ImmutableSet.copyOf(request.fieldNames);

                @Override
                public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> callback(ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> value)
                    throws Exception {
                    if (value != null && requestedFields.contains(value.getColumn()) || value == null) {
                        for (CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> requestCallback : requestCallbacks) {
                            requestCallback.callback(value);
                        }
                    }
                    return value;
                }
            }));
        }
        eventValueStore.multiRowGetAll(tenantIdAndCentricId, callbacks);
        return true;
    }

    private static class GetBatch {

        ListMultimap<GetRequest, CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>> requests = ArrayListMultimap.create();

        public void addRequest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fieldNames,
            CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> callbackStream) {
            requests.put(new GetRequest(tenantIdAndCentricId, objectId, fieldNames), callbackStream);
        }

        public ListMultimap<GetRequest, CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>> drain() {
            try {
                return requests;
            } finally {
                requests = ArrayListMultimap.create();
            }
        }
    }

    private static class GetRequest {

        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final ObjectId objectId;
        private final String[] fieldNames;

        private GetRequest(TenantIdAndCentricId tenantIdAndCentricId, ObjectId objectId, String[] fieldNames) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.objectId = objectId;
            this.fieldNames = fieldNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GetRequest that = (GetRequest) o;

            if (!Arrays.equals(fieldNames, that.fieldNames)) {
                return false;
            }
            if (!objectId.equals(that.objectId)) {
                return false;
            }
            if (!tenantIdAndCentricId.equals(that.tenantIdAndCentricId)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = tenantIdAndCentricId.hashCode();
            result = 31 * result + objectId.hashCode();
            result = 31 * result + Arrays.hashCode(fieldNames);
            return result;
        }
    }
}

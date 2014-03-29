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

import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ValueGatherer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final BatchingEventValueStore valueStore;

    public ValueGatherer(BatchingEventValueStore valueStore) {
        this.valueStore = valueStore;
    }

    public void gatherValueResults(Map<ViewDescriptor, Multimap<String, ViewValue>> allValueRequests) throws Exception {
        for (Map.Entry<ViewDescriptor, Multimap<String, ViewValue>> entry : allValueRequests.entrySet()) {
            TenantIdAndCentricId tenantIdAndCentricId = entry.getKey().getTenantIdAndCentricId();
            Multimap<String, ViewValue> valueRequests = entry.getValue();

            for (final String pathId : valueRequests.keySet()) {
                for (final ViewValue request : valueRequests.get(pathId)) {
                    valueStore.addRequest(tenantIdAndCentricId, request.getObjectId(), request.getValueFieldNames(),
                        new CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> callback(
                            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> value) throws Exception {
                            if (value != null) {
                                request.addResult(value.getColumn(), value.getValue());
                            }
                            return value;

                        }
                    });
                }
            }
        }

        valueStore.executeBatch();
    }

    public Map<ViewDescriptor, ObjectId> lookupEventIds(Map<ViewDescriptor, Set<ObjectId>> potentialIds) throws Exception {
        final Map<ViewDescriptor, ObjectId> results = new HashMap<>();
        String[] fields = new String[]{ReservedFields.INSTANCE_ID};

        for (final Map.Entry<ViewDescriptor, Set<ObjectId>> entry : potentialIds.entrySet()) {
            TenantIdAndCentricId tenantIdAndCentricId = entry.getKey().getTenantIdAndCentricId();
            for (final ObjectId eventId : entry.getValue()) {
                valueStore.addRequest(tenantIdAndCentricId, eventId, fields, new CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> callback(ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> value)
                        throws Exception {
                        if (value != null) {
                            if (value.getColumn().equals(ReservedFields.INSTANCE_ID)) {
                                ObjectId existing = results.put(entry.getKey(), eventId);
                                if (existing != null) {
                                    LOG.warn("More than one view root found for descriptor - descriptor: " + entry.getKey()
                                        + " roots: " + existing + ", " + eventId);
                                }
                            }
                        }
                        return value;
                    }
                });
            }
        }

        valueStore.executeBatch();

        return results;
    }
}

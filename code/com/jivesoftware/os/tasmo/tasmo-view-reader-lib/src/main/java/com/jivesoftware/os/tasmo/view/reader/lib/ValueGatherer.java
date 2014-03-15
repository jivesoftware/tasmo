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
import com.google.common.collect.Multimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;

/**
 *
 */
public class ValueGatherer {

    private final BatchingEventValueStore valueStore;

    public ValueGatherer(BatchingEventValueStore valueStore) {
        this.valueStore = valueStore;
    }

    public Multimap<String, OpaqueFieldValue> gatherValueResults(
        TenantIdAndCentricId tenantIdAndCentricId, Multimap<String, ValueRequest> valueRequests) throws Exception {
        
        final Multimap<String, OpaqueFieldValue> results = ArrayListMultimap.create();

        for (final String pathId : valueRequests.keySet()) {
            //TODO validate that the underlying store will return results in the order we added requests
            for (ValueRequest request : valueRequests.get(pathId)) {
                valueStore.addRequest(tenantIdAndCentricId, request.getObjectId(), request.getValueFieldNames(),
                    new CallbackStream<ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> callback(
                        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> value) throws Exception {
                        if (value != null) {
                            results.put(pathId, value.getValue());
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

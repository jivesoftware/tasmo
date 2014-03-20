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

    public void gatherValueResults(
        TenantIdAndCentricId tenantIdAndCentricId, Multimap<String, ViewValue> valueRequests) throws Exception {

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

        valueStore.executeBatch();
    }
}

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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.Reference;

/**
 *
 */
public class ReferenceGatherer {

    private BatchingReferenceStore referenceStore;

    public ReferenceGatherer(BatchingReferenceStore referenceStore) {
        this.referenceStore = referenceStore;
    }

    public void gatherReferenceResults(
        TenantIdAndCentricId tenantIdAndCentricId, Multimap<String, ViewReference> referenceRequests) throws Exception {

        for (final String pathId : referenceRequests.keySet()) {

            //TODO validate that the underlying store will return results in the order we added requests
            for (final ViewReference request : referenceRequests.get(pathId)) {
                ObjectId id = request.getOriginId();

                CallbackStream<Reference> callbackStream = new CallbackStream<Reference>() {
                    @Override
                    public Reference callback(Reference value) throws Exception {
                        if (value != null) {
                            request.addDestinationId(value);
                        }
                        return value;
                    }
                };

                if (request.isBackReference()) {
                    referenceStore.get_aIds(tenantIdAndCentricId, id, request.getOriginClassNames(), request.getRefFieldName(), callbackStream);
                } else {
                    referenceStore.get_bIds(tenantIdAndCentricId, id.getClassName(), request.getRefFieldName(), id, callbackStream);
                }
            }
        }

        referenceStore.executeBatch();

    }
}

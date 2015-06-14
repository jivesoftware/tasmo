/*
 * Copyright 2014 jonathan.
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
package com.jivesoftware.os.tasmo.reference.lib.traverser;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class SerialReferenceTraverser implements ReferenceTraverser {

    private final ReferenceStore referenceStore;

    public SerialReferenceTraverser(ReferenceStore referenceStore) {
        this.referenceStore = referenceStore;
    }

    @Override
    public void traverseForwardRef(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> classNames,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception {
        referenceStore.streamForwardRefs(tenantIdAndCentricId, classNames, fieldName, id, threadTimestamp, refStream);
    }

    @Override
    public void traversBackRefs(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> classNames,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception {
        referenceStore.streamBackRefs(tenantIdAndCentricId, id, classNames, fieldName, threadTimestamp, refStream);
    }

}

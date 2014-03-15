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
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.ClassAndField_IdKey;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class BatchingReferenceStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks;
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks;
    private final ThreadLocal<IdsBatch> bIdsBatch = new ThreadLocal<IdsBatch>() {
        @Override
        protected IdsBatch initialValue() {
            return new IdsBatch();
        }
    };
    private final ThreadLocal<IdsBatch> aIdsBatch = new ThreadLocal<IdsBatch>() {
        @Override
        protected IdsBatch initialValue() {
            return new IdsBatch();
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public BatchingReferenceStore(
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks,
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks) {
        this.multiLinks = multiLinks;
        this.multiBackLinks = multiBackLinks;
    }

    public void get_bIds(final TenantIdAndCentricId tenantIdAndCentricId,
        String aClassName, String aFieldName, ObjectId aId, final CallbackStream<Reference> versionedBIds) {

        LOG.inc("get_bIds");
        bIdsBatch.get().addReferenceRequest(tenantIdAndCentricId, Collections.singleton(aClassName), aFieldName, aId, versionedBIds);
    }

    public void get_aIds(final TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId bId, Set<String> aClassNames, String aFieldName, final CallbackStream<Reference> versionedAIds) {

        LOG.inc("get_aIds");
        aIdsBatch.get().addReferenceRequest(tenantIdAndCentricId, aClassNames, aFieldName, bId, versionedAIds);
    }

    public void get_latest_aId(final TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId bId, Set<String> aClassNames, String aFieldName, final CallbackStream<Reference> versioneAIds) throws Exception {

        LOG.inc("get_latest_aId");

        CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> callbackStream =
            new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
            final AtomicReference<ColumnValueAndTimestamp<ObjectId, byte[], Long>> latest_aId = new AtomicReference<>();

            @Override
            public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> value) throws Exception {
                if (value != null) {
                    ColumnValueAndTimestamp<ObjectId, byte[], Long> highWater = latest_aId.get();
                    if (highWater == null || value.getTimestamp() > highWater.getTimestamp()) {
                        latest_aId.set(value);
                    }
                } else {
                    ColumnValueAndTimestamp<ObjectId, byte[], Long> latest = latest_aId.get();
                    if (latest != null) {
                        versioneAIds.callback(new Reference(latest.getColumn(), latest.getTimestamp()));
                    }
                    versioneAIds.callback(null); // eos
                }
                return value;
            }
        };
        aIdsBatch.get().addRequest(tenantIdAndCentricId, aClassNames, aFieldName, bId, callbackStream);
    }

    public boolean executeBatch() throws Exception {
        boolean ranSomething = false;
        ranSomething |= runIdsBatch(bIdsBatch.get(), multiLinks);
        ranSomething |= runIdsBatch(aIdsBatch.get(), multiBackLinks);
        return ranSomething;
    }

    private boolean runIdsBatch(IdsBatch batch, RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> rcvs)
        throws Exception {
        ListMultimap<IdsRequest, CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>> requests = batch.drain();
        if (requests.isEmpty()) {
            return false;
        }
        List<KeyedColumnValueCallbackStream<ClassAndField_IdKey, ObjectId, byte[], Long>> callbacks = new ArrayList<>(requests.size());
        TenantIdAndCentricId tenantIdAndCentricId = null;
        for (Map.Entry<IdsRequest, Collection<CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>>> entry : requests.asMap().entrySet()) {
            final IdsRequest request = entry.getKey();
            final Collection<CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>> requestCallbacks = entry.getValue();
            tenantIdAndCentricId = request.tenantIdAndCentricId;
            for (String className : request.classNames) {
                final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(className, request.fieldName, request.id);
                callbacks.add(new KeyedColumnValueCallbackStream<>(aClassAndField_aId,
                    new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> value)
                        throws Exception {
                        for (CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> requestCallback : requestCallbacks) {
                            requestCallback.callback(value);
                        }
                        return value;
                    }
                }));
            }
        }
        rcvs.multiRowGetAll(tenantIdAndCentricId, callbacks);
        return true;
    }

    private static final class IdsBatch {

        ListMultimap<IdsRequest, CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>> requests = ArrayListMultimap.create();

        public void addReferenceRequest(final TenantIdAndCentricId tenantIdAndCentricId, Set<String> classNames, String fieldName, ObjectId id,
            final CallbackStream<Reference> callback) {
            requests.put(new IdsRequest(tenantIdAndCentricId, classNames, fieldName, id),
                new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                @Override
                public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> value)
                    throws Exception {
                    Reference reference = value != null ? new Reference(value.getColumn(), value.getTimestamp()) : null;
                    callback.callback(reference);
                    return value;
                }
            });
        }

        public void addRequest(final TenantIdAndCentricId tenantIdAndCentricId, Set<String> classNames, String fieldName, ObjectId id,
            CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> callback) {
            requests.put(new IdsRequest(tenantIdAndCentricId, classNames, fieldName, id), callback);
        }

        public ListMultimap<IdsRequest, CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>> drain() {
            try {
                return requests;
            } finally {
                requests = ArrayListMultimap.create();
            }
        }
    }

    private static final class IdsRequest {

        final TenantIdAndCentricId tenantIdAndCentricId;
        final Set<String> classNames;
        final String fieldName;
        final ObjectId id;

        private IdsRequest(TenantIdAndCentricId tenantIdAndCentricId, Set<String> classNames, String fieldName, ObjectId id) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.classNames = classNames;
            this.fieldName = fieldName;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IdsRequest that = (IdsRequest) o;

            if (!classNames.equals(that.classNames)) {
                return false;
            }
            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            if (!id.equals(that.id)) {
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
            result = 31 * result + classNames.hashCode();
            result = 31 * result + fieldName.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }
    }
}
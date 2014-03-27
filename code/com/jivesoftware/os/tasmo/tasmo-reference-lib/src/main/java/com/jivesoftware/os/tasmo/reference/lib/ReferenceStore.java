/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Add a link result in the following: A.a -> B @ time TA and B <- A.a @ time TA
 *
 * When standing on A the get_bId() streams B,TA
 *
 * When standing on B the get_aIds() streams A,TA
 *
 * For an A where is step 1 and we need A.a.TA and we want step 2 to B.b.TB calling get_bIds() from a concurrency perspective: B,TA is not correct. For an B
 * get_aIds() from a concurrency perspective: A,TA is correct.
 *
 */
public class ReferenceStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    static private final byte[] EMPTY = new byte[0];
    private final ConcurrencyStore concurrencyStore;
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks;
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks;

    public ReferenceStore(
            ConcurrencyStore concurrencyStore,
            RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks,
            RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks) {
        this.concurrencyStore = concurrencyStore;
        this.multiLinks = multiLinks;
        this.multiBackLinks = multiBackLinks;
    }

    public void streamForwardRefs(final TenantIdAndCentricId tenantIdAndCentricId,
            final String className,
            final String fieldName,
            final ObjectId id,
            final CallbackStream<ReferenceWithTimestamp> forwardRefs) {

        LOG.inc("get_bIds");

        final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(className, fieldName, id);
        LOG.trace(" |--> Get bIds Tenant={} A={}", tenantIdAndCentricId, aClassAndField_aId);

        multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1000, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> bId) throws Exception {
                        if (bId == null) {
                            forwardRefs.callback(null); // EOS
                            return null;
                        } else {

                            ReferenceWithTimestamp reference = new ReferenceWithTimestamp(bId.getColumn(), fieldName, bId.getTimestamp());
                            LOG.trace(" |--> Got bIds Tenant={} A={} B={} Timestamp={}", new Object[]{
                                tenantIdAndCentricId, aClassAndField_aId, bId.getColumn(), bId.getTimestamp()});

                            ReferenceWithTimestamp returned = forwardRefs.callback(reference);
                            if (returned == reference) {
                                return bId;
                            } else {
                                return null;
                            }
                        }
                    }
                });

    }

    public void streamBackRefs(final TenantIdAndCentricId tenantIdAndCentricId,
            final ObjectId id,
            final Set<String> classNames,
            final String fieldName,
            final CallbackStream<ReferenceWithTimestamp> backRefs) throws Exception {

        LOG.inc("get_aIds");

        List<KeyedColumnValueCallbackStream<ClassAndField_IdKey, ObjectId, byte[], Long>> callbacks = new ArrayList<>(classNames.size());
        for (String className : classNames) {
            final ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(className, fieldName, id);
            callbacks.add(new KeyedColumnValueCallbackStream<>(aClassAndField_bId,
                    new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> backRef)
                        throws Exception {
                            if (backRef != null) {
                                ReferenceWithTimestamp reference = new ReferenceWithTimestamp(backRef.getColumn(),
                                        fieldName, backRef.getTimestamp());
                                LOG.trace(" |--> Got aIds Tenant={} B={} A={} Timestamp={}", new Object[]{
                                    tenantIdAndCentricId, aClassAndField_bId, backRef.getColumn(), backRef.getTimestamp()});

                                ReferenceWithTimestamp returned = backRefs.callback(reference);
                                if (returned != reference) {
                                    return null;
                                }
                            }
                            return backRef;
                        }
                    }));
        }
        multiBackLinks.multiRowGetAll(tenantIdAndCentricId, callbacks);
        backRefs.callback(null); // EOS
    }

    public void streamLatestBackRef(final TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId id,
            Set<String> classNames,
            String fieldName,
            final CallbackStream<ReferenceWithTimestamp> lastestBackRefs) throws Exception {

        LOG.inc("get_latest_aId");

        final AtomicReference<ColumnValueAndTimestamp<ObjectId, byte[], Long>> latestBackRef = new AtomicReference<>();

        List<KeyedColumnValueCallbackStream<ClassAndField_IdKey, ObjectId, byte[], Long>> gets = new ArrayList<>();

        for (String className : classNames) {
            ClassAndField_IdKey rowKey = new ClassAndField_IdKey(className, fieldName, id);
            gets.add(new KeyedColumnValueCallbackStream<>(rowKey, new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                @Override
                public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(
                        ColumnValueAndTimestamp<ObjectId, byte[], Long> backRef) throws Exception {
                    if (backRef != null) {
                        ColumnValueAndTimestamp<ObjectId, byte[], Long> highWater = latestBackRef.get();
                        if (highWater == null || backRef.getTimestamp() > highWater.getTimestamp()) {
                            latestBackRef.set(backRef);
                        }
                    }
                    return backRef;
                }
            }));
        }

        multiBackLinks.multiRowGetAll(tenantIdAndCentricId, gets);

        ColumnValueAndTimestamp<ObjectId, byte[], Long> latest = latestBackRef.get();
        if (latest != null) {
            lastestBackRefs.callback(new ReferenceWithTimestamp(latest.getColumn(), fieldName, latest.getTimestamp()));
        }
        lastestBackRefs.callback(null); // eos
    }

    public void link(final TenantIdAndCentricId tenantIdAndCentricId,
            long timestamp,
            ObjectId from,
            String fieldName,
            Collection<Reference> tos) throws Exception {

        LOG.inc("link");
        LOG.startTimer("link");

        try {
            for (Reference to : tos) {

                LOG.trace("|--> link {}.{}.{}->{} t={}",
                        new Object[]{from.getClassName(), fieldName, from, to.getObjectId(), timestamp});

                ClassAndField_IdKey classAndField_from = new ClassAndField_IdKey(from.getClassName(), fieldName, from);
                ClassAndField_IdKey classAndField_to = new ClassAndField_IdKey(from.getClassName(), fieldName, to.getObjectId());

                ConstantTimestamper constantTimestamper = new ConstantTimestamper(timestamp);
                multiLinks.add(tenantIdAndCentricId, classAndField_from, to.getObjectId(), EMPTY, null, constantTimestamper);
                multiBackLinks.add(tenantIdAndCentricId, classAndField_to, from, EMPTY, null, constantTimestamper);

                LOG.trace(" |--> Set Links Tenant={} from={} to={} Timestamp={}",
                        new Object[]{tenantIdAndCentricId, classAndField_from, to, timestamp});
                LOG.trace(" |--> Set BackLinks Tenant={} to={} from={} Timestamp={}",
                        new Object[]{tenantIdAndCentricId, classAndField_to, from, timestamp});
            }
        } finally {
            LOG.stopTimer("link");
        }

        concurrencyStore.updated(tenantIdAndCentricId.getTenantId(), from, new String[]{fieldName, "deleted"}, timestamp);
    }

    public void unlink(final TenantIdAndCentricId tenantIdAndCentricId,
            final long timestamp,
            final ObjectId from,
            final String fieldName,
            final CallbackStream<ReferenceWithTimestamp> removedTos) throws Exception {

        LOG.trace("|--> un-link {}.{}.{} t={}", new Object[]{from.getClassName(), fieldName, from, timestamp});

        LOG.inc("unlink");

        final ConstantTimestamper constantTimestamper = new ConstantTimestamper(timestamp + 1);
        final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(from.getClassName(), fieldName, from);

        concurrencyStore.updated(tenantIdAndCentricId.getTenantId(), from, new String[]{fieldName, "deleted"}, timestamp);

        multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1000, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> to)
                    throws Exception {
                        if (to != null) {
                            if (to.getTimestamp() < timestamp) {

                                ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(from.getClassName(),
                                        fieldName, to.getColumn());

                                removedTos.callback(new ReferenceWithTimestamp(to.getColumn(), fieldName, to.getTimestamp()));

                                multiBackLinks.remove(tenantIdAndCentricId, aClassAndField_bId, from, constantTimestamper);
                                multiLinks.remove(tenantIdAndCentricId, aClassAndField_aId, to.getColumn(), constantTimestamper);

                            }
                        }
                        return to;
                    }
                });

        removedTos.callback(null); // EOS

    }
}

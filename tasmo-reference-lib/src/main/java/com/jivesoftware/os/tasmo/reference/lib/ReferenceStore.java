/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.RowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantKeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.TenantRowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Add a link result in the following: A.a -> B
 *
 * @ time TA and B <- A.a @ time TA
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
    private static final byte[] EMPTY = new byte[0];
    private static final int MAX_FAN_OUT_BEFORE_WARN = 10_000;
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

    public void multiStreamRefs(List<RefStreamRequestContext> refStreamRequests) throws Exception {

        List<TenantKeyedColumnValueCallbackStream<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], Long>> fowardRefStreams = new ArrayList<>();
        List<TenantKeyedColumnValueCallbackStream<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], Long>> backRefStreams = new ArrayList<>();

        for (RefStreamRequestContext refStreamRequest : refStreamRequests) {
            for (String className : refStreamRequest.getReferringClassNames()) {
                ClassAndField_IdKey cafik = new ClassAndField_IdKey(className,
                        refStreamRequest.getReferringFieldName(),
                        refStreamRequest.getReferringObjectId());

                if (refStreamRequest.isBackRefStream()) {
                    backRefStreams.add(new TenantKeyedColumnValueCallbackStream<>(refStreamRequest.getTenantIdAndCentricId(), cafik,
                            new NullSwallowingCallbackStream(refStreamRequest)));
                } else {
                    fowardRefStreams.add(new TenantKeyedColumnValueCallbackStream<>(refStreamRequest.getTenantIdAndCentricId(), cafik,
                            new NullSwallowingCallbackStream(refStreamRequest)));
                }
            }
        }
        if (!fowardRefStreams.isEmpty()) {
            multiLinks.multiRowGetAll(fowardRefStreams);
        }
        if (!backRefStreams.isEmpty()) {
            multiBackLinks.multiRowGetAll(backRefStreams);
        }

        for (RefStreamRequestContext refStreamRequest : refStreamRequests) {
            refStreamRequest.callback(null); // EOS
        }
    }


    public void streamForwardRefs(final TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> classNames,
            final String fieldName,
            final ObjectId id,
            final long threadTimestamp,
            final CallbackStream<ReferenceWithTimestamp> forwardRefs) throws Exception {

        LOG.inc("get_bIds");
        final List<ReferenceWithTimestamp> refs = new ArrayList<>();

        for (String className : classNames) {
            final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(className, fieldName, id);
            if (LOG.isTraceEnabled()) {

                LOG.trace(System.currentTimeMillis() + " |--> Get bIds Tenant={} A={}", tenantIdAndCentricId, aClassAndField_aId);
            }

            multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1_000, false, null, null,
                    new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> bId) throws Exception {
                            if (bId != null) {

                                ReferenceWithTimestamp reference = new ReferenceWithTimestamp(tenantIdAndCentricId,
                                        bId.getColumn(), fieldName, bId.getTimestamp());
                                if (LOG.isTraceEnabled()) {

                                    LOG.trace(System.currentTimeMillis() + " |--> {} Got bIds Tenant={} a={} b={} Timestamp={}", new Object[]{
                                        threadTimestamp, tenantIdAndCentricId, aClassAndField_aId, bId.getColumn(), bId.getTimestamp()});
                                }

                                refs.add(reference);

                                if (refs.size() > MAX_FAN_OUT_BEFORE_WARN) {
                                    LOG.warn("TODO: streamForwardRefs reference fan-out is exceeding comfort level. We need break scans into batched scans.");
                                }
                            }
                            return bId;
                        }
                    });

            for (ReferenceWithTimestamp ref : refs) {
                forwardRefs.callback(ref);
            }
        }
        forwardRefs.callback(null); // EOS
    }

    public void streamBackRefs(final TenantIdAndCentricId tenantIdAndCentricId,
            final ObjectId id,
            final Set<String> classNames,
            final String fieldName,
            final long threadTimestamp,
            final CallbackStream<ReferenceWithTimestamp> backRefs) throws Exception {

        LOG.inc("get_aIds");
        final List<ReferenceWithTimestamp> refs = new ArrayList<>();
        List<KeyedColumnValueCallbackStream<ClassAndField_IdKey, ObjectId, byte[], Long>> callbacks = new ArrayList<>(classNames.size());
        for (String className : classNames) {
            final ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(className, fieldName, id);
            callbacks.add(new KeyedColumnValueCallbackStream<>(aClassAndField_bId,
                    new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> backRef)
                        throws Exception {
                            if (backRef != null) {
                                ReferenceWithTimestamp reference = new ReferenceWithTimestamp(tenantIdAndCentricId, backRef.getColumn(),
                                        fieldName, backRef.getTimestamp());
                                if (LOG.isTraceEnabled()) {

                                    LOG.trace(System.currentTimeMillis() + " |--> {} Got aIds Tenant={} b={} a={} Timestamp={}", new Object[]{
                                        threadTimestamp, tenantIdAndCentricId, aClassAndField_bId, backRef.getColumn(), backRef.getTimestamp()});
                                }
                                refs.add(reference);

                                if (refs.size() > MAX_FAN_OUT_BEFORE_WARN) {
                                    LOG.warn("TODO: streamBackRefs reference fan-out is exceeding comfort level. We need break scans into batched scans.");
                                }
                            }
                            return backRef;
                        }
                    }));
        }
        multiBackLinks.multiRowGetAll(tenantIdAndCentricId, callbacks);

        for (ReferenceWithTimestamp ref : refs) {
            backRefs.callback(ref);
        }
        backRefs.callback(null); // EOS
    }

    public void link(final TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId from,
            long timestamp,
            List<LinkTo> batchLinks) throws Exception {
        LOG.inc("batchLink");
        LOG.startTimer("batchLink");

        List<String> fieldNames = new ArrayList<>(batchLinks.size() + 1);
        fieldNames.add("deleted");
        for (LinkTo link : batchLinks) {
            fieldNames.add(link.fieldName);
        }
        String[] fields = fieldNames.toArray(new String[fieldNames.size()]);
        concurrencyStore.updated(tenantIdAndCentricId, from, fields, timestamp - 1);

        try {
            List<TenantRowColumValueTimestampAdd<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[]>> links = new ArrayList<>();
            List<TenantRowColumValueTimestampAdd<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[]>> backLinks = new ArrayList<>();
            for (LinkTo link : batchLinks) {
                ClassAndField_IdKey classAndField_from = new ClassAndField_IdKey(from.getClassName(), link.fieldName, from);
                ConstantTimestamper constantTimestamper = new ConstantTimestamper(timestamp);
                for (Reference to : link.tos) {
                    links.add(new TenantRowColumValueTimestampAdd<>(tenantIdAndCentricId, classAndField_from, to.getObjectId(), EMPTY, constantTimestamper));
                    ClassAndField_IdKey classAndField_to = new ClassAndField_IdKey(from.getClassName(), link.fieldName, to.getObjectId());
                    backLinks.add(new TenantRowColumValueTimestampAdd<>(tenantIdAndCentricId, classAndField_to, from, EMPTY, constantTimestamper));
                }
            }
            multiLinks.multiRowsMultiAdd(links);
            multiBackLinks.multiRowsMultiAdd(backLinks);

        } finally {
            LOG.stopTimer("batchLink");
        }

        concurrencyStore.updated(tenantIdAndCentricId, from, fields, timestamp);
    }


    public void unlink(final TenantIdAndCentricId tenantIdAndCentricId,
            final long timestamp,
            final ObjectId from,
            final String fieldName,
            final long threadTimestamp,
            final CallbackStream<ReferenceWithTimestamp> removedTos) throws Exception {

        if (LOG.isTraceEnabled()) {
            LOG.trace("|--> un-link {}.{}.{} t={}", new Object[]{from.getClassName(), fieldName, from, timestamp});
        }
        LOG.inc("unlink");

        final ConstantTimestamper constantTimestamper = new ConstantTimestamper(timestamp + 1);
        final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(from.getClassName(), fieldName, from);

        concurrencyStore.updated(tenantIdAndCentricId, from, new String[]{fieldName, "deleted"}, timestamp - 1);

        final List<ReferenceWithTimestamp> tos = new ArrayList<>();
        final List<RowColumnTimestampRemove<ClassAndField_IdKey, ObjectId>> removeBackLinks = new ArrayList<>();
        final List<RowColumnTimestampRemove<ClassAndField_IdKey, ObjectId>> removeLinks = new ArrayList<>();

        multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1_000, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> to)
                    throws Exception {
                        if (to != null) {
                            if (to.getTimestamp() < timestamp) {

                                ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(from.getClassName(),
                                        fieldName, to.getColumn());

                                tos.add(new ReferenceWithTimestamp(tenantIdAndCentricId, to.getColumn(), fieldName, to.getTimestamp()));

                                removeBackLinks.add(new RowColumnTimestampRemove<>(aClassAndField_bId, from, constantTimestamper));
                                removeLinks.add(new RowColumnTimestampRemove<>(aClassAndField_aId, to.getColumn(), constantTimestamper));

                                if (tos.size() > MAX_FAN_OUT_BEFORE_WARN) {
                                    LOG.warn("TODO: unlink reference fan-out is exceeding comfort level. We need break scans into batched scans.");
                                }
                            }
                        }
                        return to;
                    }
                });

        for (ReferenceWithTimestamp to : tos) {
            removedTos.callback(to);
        }

        multiBackLinks.multiRowsMultiRemove(tenantIdAndCentricId, removeBackLinks);
        multiLinks.multiRowsMultiRemove(tenantIdAndCentricId, removeLinks);

        concurrencyStore.updated(tenantIdAndCentricId, from, new String[]{fieldName, "deleted"}, timestamp);

        removedTos.callback(null); // EOS

    }

    /**
     * prevents delegate from getting 'null' aka end of stream marker
     */
    static class NullSwallowingCallbackStream implements CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> {

        private final CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> delagate;

        NullSwallowingCallbackStream(
            CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> delagate) {
            this.delagate = delagate;
        }

        @Override
        public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> v) throws Exception {
            if (v != null) {
                return delagate.callback(v);
            }
            return v;
        }
    }

    public static class LinkTo {

        private final String fieldName;
        private final Collection<Reference> tos;

        public LinkTo(String fieldName, Collection<Reference> tos) {
            this.fieldName = fieldName;
            this.tos = tos;
        }
    }
}

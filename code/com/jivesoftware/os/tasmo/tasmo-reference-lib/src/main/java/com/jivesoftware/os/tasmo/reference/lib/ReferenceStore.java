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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ReferenceStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    static private final byte[] EMPTY = new byte[0];
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks;
    private final RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks;

    public ReferenceStore(
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiLinks,
        RowColumnValueStore<TenantIdAndCentricId, ClassAndField_IdKey, ObjectId, byte[], RuntimeException> multiBackLinks) {
        this.multiLinks = multiLinks;
        this.multiBackLinks = multiBackLinks;
    }

    public void get_bIds(final TenantIdAndCentricId tenantIdAndCentricId,
        String aClassName, String aFieldName, Reference aId, final CallbackStream<Reference> versionedBIds) {

        LOG.inc("get_bIds");

        final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(aClassName, aFieldName, aId.getObjectId());
        LOG.trace(" |--> Get bIds Tenant={} A={}", tenantIdAndCentricId, aClassAndField_aId);

        multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
            @Override
            public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> value) throws Exception {
                if (value == null) {
                    versionedBIds.callback(null); // EOS
                    return null;
                } else {
                    Reference reference = new Reference(value.getColumn(), value.getTimestamp());
                    LOG.trace(" |--> Got bIds Tenant={} A={} B={} Timestamp={}", new Object[]{
                        tenantIdAndCentricId, aClassAndField_aId, value.getColumn(), value.getTimestamp()});
                    Reference returned = versionedBIds.callback(reference);
                    if (returned == reference) {
                        return value;
                    } else {
                        return null;
                    }
                }
            }
        });

    }

    public void get_aIds(final TenantIdAndCentricId tenantIdAndCentricId,
        Reference bId, Set<String> aClassNames, String aFieldName, final CallbackStream<Reference> versioneAIds) throws Exception {

        LOG.inc("get_aIds");

        for (String aClassName : aClassNames) {

            final ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(aClassName, aFieldName, bId.getObjectId());
            LOG.trace(" |--> Get aIds Tenant={} B={}", tenantIdAndCentricId, aClassAndField_bId);
            multiBackLinks.getEntrys(tenantIdAndCentricId, aClassAndField_bId, null, Long.MAX_VALUE, 1000, false, null, null,
                new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                @Override
                public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> value) throws Exception {
                    if (value != null) {
                        Reference reference = new Reference(value.getColumn(), value.getTimestamp());
                        LOG.trace(" |--> Got aIds Tenant={} B={} A={} Timestamp={}", new Object[]{
                            tenantIdAndCentricId, aClassAndField_bId, value.getColumn(), value.getTimestamp()});
                        Reference returned = versioneAIds.callback(reference);
                        if (returned != reference) {
                            return null;
                        }
                    }

                    return value;
                }
            });
        }

        versioneAIds.callback(null); // EOS
    }

    public void get_latest_aId(final TenantIdAndCentricId tenantIdAndCentricId,
        Reference bId,
        Set<String> aClassNames,
        String aFieldName,
        final CallbackStream<Reference> versioneAIds) throws Exception {

        LOG.inc("get_latest_aId");

        final AtomicReference<ColumnValueAndTimestamp<ObjectId, byte[], Long>> latest_aId = new AtomicReference<>();

        List<KeyedColumnValueCallbackStream<ClassAndField_IdKey, ObjectId, byte[], Long>> gets = new ArrayList<>();

        for (String aClassName : aClassNames) {
            ClassAndField_IdKey rowKey = new ClassAndField_IdKey(aClassName, aFieldName, bId.getObjectId());
            gets.add(new KeyedColumnValueCallbackStream<>(rowKey, new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
                @Override
                public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(
                    ColumnValueAndTimestamp<ObjectId, byte[], Long> value) throws Exception {
                    if (value != null) {
                        ColumnValueAndTimestamp<ObjectId, byte[], Long> highWater = latest_aId.get();
                        if (highWater == null || value.getTimestamp() > highWater.getTimestamp()) {
                            latest_aId.set(value);
                        }
                    }
                    return value;
                }
            }));
        }

        multiBackLinks.multiRowGetAll(tenantIdAndCentricId, gets);

        ColumnValueAndTimestamp<ObjectId, byte[], Long> latest = latest_aId.get();
        if (latest != null) {
            versioneAIds.callback(new Reference(latest.getColumn(), latest.getTimestamp()));
        }
        versioneAIds.callback(null); // eos
    }

    public void link_aId_aField_to_bIds(final TenantIdAndCentricId tenantIdAndCentricId,
        long addAtTimestamp, Reference aId, String aFieldName, Collection<Reference> bIds) throws Exception {

        LOG.inc("link_aId_aField_to_bIds");
        LOG.startTimer("link_aId_aField_to_bIds");

        try {
            for (Reference bId : bIds) {

                ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(aId.getObjectId().getClassName(), aFieldName, aId.getObjectId());
                ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(aId.getObjectId().getClassName(), aFieldName, bId.getObjectId());

                ConstantTimestamper constantTimestamper = new ConstantTimestamper(addAtTimestamp);
                multiLinks.add(tenantIdAndCentricId, aClassAndField_aId, bId.getObjectId(), EMPTY, null, constantTimestamper);
                multiBackLinks.add(tenantIdAndCentricId, aClassAndField_bId, aId.getObjectId(), EMPTY, null, constantTimestamper);

                LOG.trace(" |--> Set Links Tenant={} A={} B={} Timestamp={}",
                    new Object[]{tenantIdAndCentricId, aClassAndField_aId, bId, addAtTimestamp});
                LOG.trace(" |--> Set BackLinks Tenant={} B={} A={} Timestamp={}",
                    new Object[]{tenantIdAndCentricId, aClassAndField_bId, aId, addAtTimestamp});
            }
        } finally {
            LOG.stopTimer("link_aId_aField_to_bIds");
        }
    }

    public void remove_aId_aField(final TenantIdAndCentricId tenantIdAndCentricId,
        final long removeAtTimestamp,
        final Reference aId,
        final String aFieldName,
        final CallbackStream<Reference> removed_bIds) throws Exception {

        LOG.inc("remove_aId_aField");

        final ConstantTimestamper constantTimestamper = new ConstantTimestamper(removeAtTimestamp);
        final ClassAndField_IdKey aClassAndField_aId = new ClassAndField_IdKey(aId.getObjectId().getClassName(), aFieldName, aId.getObjectId());

        multiLinks.getEntrys(tenantIdAndCentricId, aClassAndField_aId, null, Long.MAX_VALUE, 1000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>>() {
            @Override
            public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> bObjectId)
                throws Exception {
                if (bObjectId != null) {
                    if (bObjectId.getTimestamp() < removeAtTimestamp) {

                        ClassAndField_IdKey aClassAndField_bId = new ClassAndField_IdKey(aId.getObjectId().getClassName(),
                            aFieldName, bObjectId.getColumn());
                        multiLinks.remove(tenantIdAndCentricId, aClassAndField_aId, bObjectId.getColumn(), constantTimestamper);
                        multiBackLinks.remove(tenantIdAndCentricId, aClassAndField_bId, aId.getObjectId(), constantTimestamper);

                        removed_bIds.callback(new Reference(bObjectId.getColumn(), bObjectId.getTimestamp()));
                    }
                }
                return bObjectId;
            }
        });

        removed_bIds.callback(null); // EOS

    }
}

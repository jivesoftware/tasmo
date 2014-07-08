package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan
 */
public class RefStreamRequestContext implements CallbackStream<ColumnValueAndTimestamp<ObjectId, byte[], Long>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int MAX_FAN_OUT_BEFORE_WARN = 10_000;

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final Set<String> referringClassNames;
    private final String referringFieldName;
    private final ObjectId referringObjectId;
    private final long readTime;
    private final boolean backRefStream;

    private final List<ReferenceWithTimestamp> NULL = Collections.emptyList();
    private final BlockingQueue<List<ReferenceWithTimestamp>> refStreamQueue = new LinkedBlockingQueue<>();
    private final List<ReferenceWithTimestamp> batch = new ArrayList<>();
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public RefStreamRequestContext(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> referringClassName,
            String referringFieldName,
            ObjectId referringObjectId,
            long readTime,
            boolean backRefStream) {

        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.referringClassNames = referringClassName;
        this.referringFieldName = referringFieldName;
        this.referringObjectId = referringObjectId;
        this.readTime = readTime;
        this.backRefStream = backRefStream;
    }

    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return tenantIdAndCentricId;
    }

    public Set<String> getReferringClassNames() {
        return Collections.unmodifiableSet(referringClassNames);
    }

    public String getReferringFieldName() {
        return referringFieldName;
    }

    public ObjectId getReferringObjectId() {
        return referringObjectId;
    }

    public long getReadTime() {
        return readTime;
    }

    public boolean isBackRefStream() {
        return backRefStream;
    }

    @Override
    public ColumnValueAndTimestamp<ObjectId, byte[], Long> callback(ColumnValueAndTimestamp<ObjectId, byte[], Long> v) throws Exception {
        if (v == null) {
            if (!batch.isEmpty()) {
                refStreamQueue.put(batch);
            }
            refStreamQueue.put(NULL);
        } else {
            ReferenceWithTimestamp referenceWithTimestamp = new ReferenceWithTimestamp(v.getColumn(), referringFieldName, v.getTimestamp());
            batch.add(referenceWithTimestamp);
            if (batch.size() > MAX_FAN_OUT_BEFORE_WARN) {
                LOG.warn("TODO: streamBackRefs reference fan-out is exceeding comfort level. We need to break scans into batched scans.");
            }
        }
        return v;
    }

    public void failure(Exception cause) {
        if (!failure.compareAndSet(null, cause)) {
            throw new RuntimeException("Trying to fail somethig that has already failed");
        }
    }

    public void traverse(CallbackStream<ReferenceWithTimestamp> refStream) throws Exception {
        while (true) {
            Exception cause = failure.get();
            if (cause != null) {
                throw cause;
            }
            List<ReferenceWithTimestamp> took = refStreamQueue.take();
            if (took == NULL) {
                refStream.callback(null);
                return;
            } else {
                for (ReferenceWithTimestamp t : took) {
                    refStream.callback(t);
                }
            }
        }
    }
}

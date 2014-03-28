/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.service.writer;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author pete
 */
public class ViewValueWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewValueStore viewValueStore;

    public ViewValueWriter(ViewValueStore viewValueStore) {
        this.viewValueStore = viewValueStore;
    }

    public Transaction begin(TenantIdAndCentricId tenantIdAndCentricId) {
        return new Transaction(tenantIdAndCentricId);
    }

    public void commit(Transaction transaction) throws IOException {
        TenantIdAndCentricId tenantId = transaction.tenantIdAndCentricId;

        List<ViewWriteFieldChange> adds = transaction.takeAdds();
        if (!adds.isEmpty()) {
            LOG.inc("added view paths", adds.size());

            viewValueStore.add(tenantId, adds);

            if (LOG.isTraceEnabled()) {
                for (ViewWriteFieldChange add : adds) {
                    LOG.trace(add.toString());
                }
            }
        }
        List<ViewWriteFieldChange> removes = transaction.takeRemoves();
        if (!removes.isEmpty()) {
            LOG.inc("removed view paths", adds.size());

            viewValueStore.remove(tenantId, removes);

            if (LOG.isTraceEnabled()) {
                for (ViewWriteFieldChange remove : removes) {
                    LOG.trace(remove.toString());
                }
            }
        }

    }

    /**
     * These cannot be shared across multiple threads.
     */
    final public static class Transaction {

        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final Thread constructingThread;
        private final List<ViewWriteFieldChange> adds;
        private final List<ViewWriteFieldChange> removes;

        private Transaction(TenantIdAndCentricId tenantIdAndCentricId) {
            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.adds = new LinkedList<>();
            this.removes = new LinkedList<>();
            this.constructingThread = Thread.currentThread();
        }

        public void set(ObjectId viewObjectId, String modelPathId, ObjectId[] modelPathInstanceIds, String value, long timestamp) throws IOException {
            if (!Thread.currentThread().equals(constructingThread)) {
                throw new IllegalStateException("calling thread must be the same as creating thread.");
            }
            adds.add(new ViewWriteFieldChange(
                0, // not used,
                tenantIdAndCentricId,
                new Id(0), // not used
                ViewWriteFieldChange.Type.add,
                viewObjectId,
                modelPathId,
                modelPathInstanceIds,
                value,
                timestamp));
        }

        public void remove(ObjectId viewObjectId, String modelPathId, ObjectId[] modelPathInstanceIds, long timestamp) throws IOException {
            if (!Thread.currentThread().equals(constructingThread)) {
                throw new IllegalStateException("calling thread must be the same as creating thread.");
            }
            removes.add(new ViewWriteFieldChange(
                0, // not used
                tenantIdAndCentricId,
                new Id(0), // not used
                ViewWriteFieldChange.Type.remove,
                viewObjectId,
                modelPathId,
                modelPathInstanceIds,
                null,
                timestamp));
        }

        List<ViewWriteFieldChange> takeAdds() {
            try {
                if (!Thread.currentThread().equals(constructingThread)) {
                    throw new IllegalStateException("calling thread must be the same as creating thread.");
                }
                return new ArrayList<>(adds);
            } finally {
                adds.clear();
            }
        }

        List<ViewWriteFieldChange> takeRemoves() {
            try {
                if (!Thread.currentThread().equals(constructingThread)) {
                    throw new IllegalStateException("calling thread must be the same as creating thread.");
                }
                return new ArrayList<>(removes);
            } finally {
                removes.clear();
            }
        }
    }
}

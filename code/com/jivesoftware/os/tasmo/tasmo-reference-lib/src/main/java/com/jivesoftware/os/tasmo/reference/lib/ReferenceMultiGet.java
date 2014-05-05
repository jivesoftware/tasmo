package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantKeyedColumnValueCallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.TenantRowColumnTimestampRemove;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author jonathan.colt
 */
public class ReferenceMultiGet {

    private final ReferenceStore referenceStore;
    private final Queue<ReferenceRequest> referenceRequests = new LinkedBlockingQueue<>();
    private final int maxBatchSize;

    public ReferenceMultiGet(ReferenceStore referenceStore, int maxBatchSize) {
        this.referenceStore = referenceStore;
        this.maxBatchSize = maxBatchSize;
    }


    public void add(ReferenceRequest referenceRequest) {

    }

    public void pump() {
        List<ReferenceRequest> batch = new ArrayList<>();
        while(batch.size() < maxBatchSize) {
            ReferenceRequest referenceRequest = referenceRequests.poll();
            if (referenceRequest == null) {
                break;
            }
            batch.add(referenceRequest);
        }

        // TODO multiGet();

        List<TenantKeyedColumnValueCallbackStream> get = new ArrayList<>();
        get.add(new TenantKeyedColumnValueCallbackStream(get, get, null));

    }

    public static class ReferenceRequest {
        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final ObjectId referringObjectId;
        private final long readTime;
        private final CallbackStream<ReferenceWithTimestamp> referencedIdsStream;

        public ReferenceRequest(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            long readTime,
            CallbackStream<ReferenceWithTimestamp> referencedIdsStream) {

            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.referringObjectId = referringObjectId;
            this.readTime = readTime;
            this.referencedIdsStream = referencedIdsStream;
        }

    }

    public static class ReferenceResponse {
        private final TenantIdAndCentricId tenantIdAndCentricId;
        private final ObjectId referringObjectId;
        private final long readTime;
        private final List<ReferenceWithTimestamp> response;

        public ReferenceResponse(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            long readTime,
            List<ReferenceWithTimestamp> response) {

            this.tenantIdAndCentricId = tenantIdAndCentricId;
            this.referringObjectId = referringObjectId;
            this.readTime = readTime;
            this.response = response;
        }
    }
}

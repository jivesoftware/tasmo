package com.jivesoftware.os.tasmo.reference.lib.traverser;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamRequestContext;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author jonathan
 */
public class BatchingReferenceTraverser implements ReferenceTraverser {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ReferenceStore referenceStore;
    private final ListeningExecutorService traverserExecutors;
    private final int processUpToNRequestAtATime;
    private final BlockingQueue<RefStreamRequestContext> requestsQueue;

    public BatchingReferenceTraverser(ReferenceStore referenceStore,
            ListeningExecutorService traverserExecutors,
            int processUpToNRequestAtATime,
            int requestQueueCapacity) {
        this.referenceStore = referenceStore;
        this.traverserExecutors = traverserExecutors;
        this.processUpToNRequestAtATime = processUpToNRequestAtATime;
        this.requestsQueue = new ArrayBlockingQueue<>(requestQueueCapacity);
    }

    public void processRequests() throws InterruptedException {
        while (true) {
            final List<RefStreamRequestContext> requests = new ArrayList<>();
            RefStreamRequestContext took = requestsQueue.take();
            requests.add(took);
            requestsQueue.drainTo(requests, processUpToNRequestAtATime);
            if (requests.isEmpty()) {
                return;
            }
            traverserExecutors.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (requests.size() > 1) {
                            LOG.info("Request aggregation size:" + requests.size());
                        }
                        referenceStore.multiStreamRefs(requests);
                    } catch (Exception ex) {
                        LOG.warn("Failed to process request:" + requests, ex);
                        for (RefStreamRequestContext request : requests) {
                            request.failure(ex);
                        }
                    }
                }
            });
        }
    }

    @Override
    public void traverseForwardRef(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> classNames,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception {

        RefStreamRequestContext refStreamRequestContext = new RefStreamRequestContext(tenantIdAndCentricId,
                classNames,
                fieldName,
                id,
                threadTimestamp,
                false);

        requestsQueue.put(refStreamRequestContext);

        refStreamRequestContext.traverse(refStream);
    }

    @Override
    public void traversBackRefs(TenantIdAndCentricId tenantIdAndCentricId,
            Set<String> classNames,
            String fieldName,
            ObjectId id,
            long threadTimestamp,
            CallbackStream<ReferenceWithTimestamp> refStream) throws InterruptedException, Exception {

        RefStreamRequestContext refStreamRequestContext = new RefStreamRequestContext(tenantIdAndCentricId,
                classNames,
                fieldName,
                id,
                threadTimestamp,
                true);

        requestsQueue.put(refStreamRequestContext);

        refStreamRequestContext.traverse(refStream);
    }

}

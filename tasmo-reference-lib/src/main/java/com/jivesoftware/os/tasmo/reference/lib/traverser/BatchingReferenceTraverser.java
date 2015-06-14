package com.jivesoftware.os.tasmo.reference.lib.traverser;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamRequestContext;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean running = new AtomicBoolean(false);

    public BatchingReferenceTraverser(ReferenceStore referenceStore,
            ListeningExecutorService traverserExecutors,
            int processUpToNRequestAtATime,
            int requestQueueCapacity) {
        this.referenceStore = referenceStore;
        this.traverserExecutors = traverserExecutors;
        this.processUpToNRequestAtATime = processUpToNRequestAtATime;
        this.requestsQueue = new ArrayBlockingQueue<>(requestQueueCapacity);
    }

    public void startProcessingRequests() throws InterruptedException {
        if (running.compareAndSet(false, true)) {
            while (running.get()) {
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
                                LOG.debug("Request aggregation size: {}", requests.size());
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
    }

    public void stopProcessingRequests() {
        running.compareAndSet(true, false);
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

package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathConsistencyException;

/**
 *
 * @author jonathan
 */
public class TasmoRetryingEventTraverser {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WrittenEventProcessorDecorator writtenEventProcessorDecorator;
    private final OrderIdProvider threadTime;

    public TasmoRetryingEventTraverser(WrittenEventProcessorDecorator writtenEventProcessorDecorator, OrderIdProvider threadTime) {
        this.writtenEventProcessorDecorator = writtenEventProcessorDecorator;
        this.threadTime = threadTime;
    }

    public void traverseEvent(InitiateTraversal initiateTraversal,
            WrittenEventContext writtenEventContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent) throws RuntimeException, Exception {

        String instanceClassName = writtenEvent.getWrittenInstance().getInstanceId().getClassName();
        long startConsistency = System.currentTimeMillis();

        int attempts = 0;
        int maxAttempts = 3; // TODO expose to config
        while (attempts < maxAttempts) {
            attempts++;
            try {
                long start = System.currentTimeMillis();
                WrittenEventProcessor writtenEventProcessor = writtenEventProcessorDecorator.decorateWrittenEventProcessor(initiateTraversal);
                writtenEventProcessor.process(writtenEventContext, tenantIdAndCentricId, writtenEvent, threadTime.nextId());
                writtenEventContext.getProcessingStats().latency("EVENT TRAVERSAL", instanceClassName, System.currentTimeMillis() - start);
                break;
            } catch (Exception e) {
                boolean pathModifiedException = false;
                Throwable t = e;
                while (t != null) {
                    if (t instanceof PathConsistencyException) {
                        pathModifiedException = true;
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("** RETRY ** " + t.toString(), t);
                        }
                    }
                    t = t.getCause();
                }
                if (pathModifiedException) {
                    Thread.sleep(100); // TODO is yield a better choice?
                    LOG.inc("pathModifiedOutFromUnderneathMe>all");
                    LOG.inc("pathModifiedOutFromUnderneathMe>" + instanceClassName);
                } else {
                    throw e;
                }
            }
        }
        writtenEventContext.getProcessingStats().tally("CONSISTENCY ATTEMPTS", instanceClassName, attempts - 1);

        if (attempts >= maxAttempts) {
            LOG.info("FAILED to reach CONSISTENCY after {} attempts for eventId:{} instanceId:{} tenantId:{}", new Object[]{
                attempts,
                writtenEvent.getEventId(),
                writtenEvent.getWrittenInstance().getInstanceId(),
                writtenEvent.getTenantId()});
            throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts.");
        } else {
            if (attempts > 1) {
                LOG.warn("CONSISTENCY took {} attempts for eventId:{} instanceId:{} tenantId:{}", new Object[]{
                    attempts,
                    writtenEvent.getEventId(),
                    writtenEvent.getWrittenInstance().getInstanceId(),
                    writtenEvent.getTenantId()
                });
            }
        }

        writtenEventContext.getProcessingStats().latency("EVENT CONSISTENCY", instanceClassName, System.currentTimeMillis() - startConsistency);
    }
}

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
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;

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
            WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent) throws RuntimeException, Exception {

        int attempts = 0;
        int maxAttempts = 10; // TODO expose to config
        while (attempts < maxAttempts) {
            attempts++;
            if (attempts > 1) {
                LOG.info("attempts " + attempts);
            }
            try {

                WrittenEventProcessor writtenEventProcessor = writtenEventProcessorDecorator.decorateWrittenEventProcessor(initiateTraversal);
                writtenEventProcessor.process(batchContext, tenantIdAndCentricId, writtenEvent, threadTime.nextId());
                break;
            } catch (Exception e) {
                boolean pathModifiedException = false;
                Throwable t = e;
                while (t != null) {
                    if (t instanceof PathModifiedOutFromUnderneathMeException) {
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
                    LOG.inc("pathModifiedOutFromUnderneathMe>" + writtenEvent.getWrittenInstance().getInstanceId().getClassName());
                } else {
                    throw e;
                }
            }
        }
        if (attempts >= maxAttempts) {
            throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts.");
        }
    }
}

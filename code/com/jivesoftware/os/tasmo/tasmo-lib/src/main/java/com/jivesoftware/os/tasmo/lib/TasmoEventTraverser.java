package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

/**
 *
 * @author jonathan
 */
public class TasmoEventTraverser implements TasmoEventTraversal {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final WrittenEventProcessorDecorator writtenEventProcessorDecorator;
    private final OrderIdProvider threadTime;

    public TasmoEventTraverser(WrittenEventProcessorDecorator writtenEventProcessorDecorator, OrderIdProvider threadTime) {
        this.writtenEventProcessorDecorator = writtenEventProcessorDecorator;
        this.threadTime = threadTime;
    }

    @Override
    public void traverseEvent(WrittenEventProcessor writtenEventProcessor,
        WrittenEventContext writtenEventContext,
        TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent) throws RuntimeException, Exception {

        String instanceClassName = writtenEvent.getWrittenInstance().getInstanceId().getClassName();

        long start = System.currentTimeMillis();
        WrittenEventProcessor decoratedWrittenEventProcessor
            = writtenEventProcessorDecorator.decorateWrittenEventProcessor(writtenEventProcessor);
        decoratedWrittenEventProcessor.process(writtenEventContext, tenantIdAndCentricId, writtenEvent, threadTime.nextId());
        writtenEventContext.getProcessingStats().latency("EVENT TRAVERSAL", instanceClassName, System.currentTimeMillis() - start);

    }
}

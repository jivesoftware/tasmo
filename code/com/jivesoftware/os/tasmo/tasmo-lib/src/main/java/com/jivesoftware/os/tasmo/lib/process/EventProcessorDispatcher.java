/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public class EventProcessorDispatcher implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final String className;
    private final ValueProcessor valueProcessor;
    private final RefProcessor refProcessor;
    private final RefProcessor refsProcessor;
    private final AllBackrefsProcessor allBackrefsProcessor;
    private final LatestBackrefProcessor latestBackrefProcessor;
    private final RefRemovalProcessor refRemovalProcessor;
    private final RefRemovalProcessor refsRemovalProcessor;
    private final BackrefRemovalProcessor latestBackRefRemovalProcessor;
    private final BackrefRemovalProcessor allBackRefsRemovalProcessor;
    private final BackrefRemovalProcessor countsRemovalProcessor;
    private final ExistenceTransitionProcessor existenceTransitionProcessor;

    public EventProcessorDispatcher(String className,
        ValueProcessor valueProcessor,
        RefProcessor refProcessor,
        RefProcessor refsProcessor,
        AllBackrefsProcessor allBackrefsProcessor,
        LatestBackrefProcessor latestBackrefProcessor,
        RefRemovalProcessor refRemovalProcessor,
        RefRemovalProcessor refsRemovalProcessor,
        BackrefRemovalProcessor latestBackRefRemovalProcessor,
        BackrefRemovalProcessor allBackRefsRemovalProcessor,
        BackrefRemovalProcessor countsRemovalProcessor,
        ExistenceTransitionProcessor existenceTransitionProcessor) {
        this.className = className;
        this.valueProcessor = valueProcessor;
        this.refProcessor = refProcessor;
        this.refsProcessor = refsProcessor;
        this.allBackrefsProcessor = allBackrefsProcessor;
        this.latestBackrefProcessor = latestBackrefProcessor;
        this.refRemovalProcessor = refRemovalProcessor;
        this.refsRemovalProcessor = refsRemovalProcessor;
        this.latestBackRefRemovalProcessor = latestBackRefRemovalProcessor;
        this.allBackRefsRemovalProcessor = allBackRefsRemovalProcessor;
        this.countsRemovalProcessor = countsRemovalProcessor;
        this.existenceTransitionProcessor = existenceTransitionProcessor;
    }

    @Override
    public boolean process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {

        LOG.trace("Start:" + this.toString());
        boolean wasProcessed = false;
        wasProcessed |= invokeEventProcessor(batchContext, "values", valueProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "refs", refProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "multi-refs", refsProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "back-refs", allBackrefsProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "latest back-refs", latestBackrefProcessor, writtenEvent);

        wasProcessed |= invokeEventProcessor(batchContext, "ref removals", refRemovalProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "multi-ref removals", refsRemovalProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "back-ref removals", allBackRefsRemovalProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "count removals", countsRemovalProcessor, writtenEvent);
        wasProcessed |= invokeEventProcessor(batchContext, "latest back-ref removals", latestBackRefRemovalProcessor, writtenEvent);

        if (batchContext.getTransitioning().contains(writtenEvent.getWrittenInstance().getInstanceId())) {
            wasProcessed |= invokeEventProcessor(batchContext, "existence transition", existenceTransitionProcessor, writtenEvent);
        }

        LOG.trace("End:" + this.toString());
        return wasProcessed;
    }

    private boolean invokeEventProcessor(WrittenEventContext batchContext,
        String processorName,
        EventProcessor eventProcessor,
        WrittenEvent writtenEvent) throws Exception {
        if (eventProcessor != null) {
            LOG.startTimer(processorName);
            try {
                return eventProcessor.process(batchContext, writtenEvent);
            } finally {
                LOG.stopTimer(processorName);
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "InitialStepDispatcher{" + "className=" + className + '}';
    }
}

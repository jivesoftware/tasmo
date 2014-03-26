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
import com.jivesoftware.os.tasmo.lib.EventWrite;

public class EventProcessorDispatcher implements EventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ValueProcessor valueProcessor;
    private final RefProcessor refProcessor;

    public EventProcessorDispatcher(
        ValueProcessor valueProcessor,
        RefProcessor refProcessor) {
        this.valueProcessor = valueProcessor;
        this.refProcessor = refProcessor;

    }

    @Override
    public boolean process(EventWrite write) throws Exception {

        LOG.trace("Start:" + this.toString());
        boolean wasProcessed = false;
        wasProcessed |= invokeEventProcessor("values", valueProcessor, write);
        wasProcessed |= invokeEventProcessor("refs", refProcessor, write);
        LOG.trace("End:" + this.toString());
        return wasProcessed;
    }

    private boolean invokeEventProcessor(
        String processorName,
        EventProcessor eventProcessor,
        EventWrite write) throws Exception {
        if (eventProcessor != null) {
            LOG.startTimer(processorName);
            try {
                return eventProcessor.process(write);
            } finally {
                LOG.stopTimer(processorName);
            }
        } else {
            return false;
        }
    }
}

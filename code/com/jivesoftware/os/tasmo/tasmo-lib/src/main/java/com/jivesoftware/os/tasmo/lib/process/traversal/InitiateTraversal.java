/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public class InitiateTraversal implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final String className;
    private final InitiateValueTraversal valueTraverser;
    private final InitiateRefTraversal refTraverser;

    public InitiateTraversal(String className,
            InitiateValueTraversal valueTraverser,
            InitiateRefTraversal refTraverser) {
        this.className = className;
        this.valueTraverser = valueTraverser;
        this.refTraverser = refTraverser;
    }

    @Override
    public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent, long threadTimestamp) throws Exception {

        invokeEventTraverser(batchContext, "values", valueTraverser, writtenEvent, threadTimestamp);
        invokeEventTraverser(batchContext, "refs", refTraverser, writtenEvent, threadTimestamp);

    }

    private void invokeEventTraverser(WrittenEventContext batchContext,
            String processorName,
            EventProcessor eventProcessor,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {
        LOG.startTimer(processorName);
        try {
            eventProcessor.process(batchContext, writtenEvent, threadTimestamp);
        } finally {
            LOG.stopTimer(processorName);
        }
    }

    @Override
    public String toString() {
        return "InitiateTraversal{" + "className=" + className + '}';
    }
}

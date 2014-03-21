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
    private final InitiateRefTraversal refsTraverser;
    private final InitiateBackRefTraversal allBackrefsTraverser;
    private final InitiateBackRefTraversal latestBackrefTraverser;
    private final InitiateRefRemovalTraversal refRemovalTraverser;
    private final InitiateRefRemovalTraversal refsRemovalTraverser;
    private final InitiateBackrefRemovalTraversal latestBackRefRemovalTraverser;
    private final InitiateBackrefRemovalTraversal allBackRefsRemovalTraverser;
    private final InitiateBackrefRemovalTraversal countsRemovalTraverser;
    private final InitiateExistenceTransitionTraversal existenceTransitionTraverser;

    public InitiateTraversal(String className,
            InitiateValueTraversal valueTraverser,
            InitiateRefTraversal refTraverser,
            InitiateRefTraversal refsTraverser,
            InitiateBackRefTraversal allBackrefsTraverser,
            InitiateBackRefTraversal latestBackrefTraverser,
            InitiateRefRemovalTraversal refRemovalTraverser,
            InitiateRefRemovalTraversal refsRemovalTraverser,
            InitiateBackrefRemovalTraversal latestBackRefRemovalTraverser,
            InitiateBackrefRemovalTraversal allBackRefsRemovalTraverser,
            InitiateBackrefRemovalTraversal countsRemovalTraverser,
            InitiateExistenceTransitionTraversal existenceTransitionTraverser) {
        this.className = className;
        this.valueTraverser = valueTraverser;
        this.refTraverser = refTraverser;
        this.refsTraverser = refsTraverser;
        this.allBackrefsTraverser = allBackrefsTraverser;
        this.latestBackrefTraverser = latestBackrefTraverser;
        this.refRemovalTraverser = refRemovalTraverser;
        this.refsRemovalTraverser = refsRemovalTraverser;
        this.latestBackRefRemovalTraverser = latestBackRefRemovalTraverser;
        this.allBackRefsRemovalTraverser = allBackRefsRemovalTraverser;
        this.countsRemovalTraverser = countsRemovalTraverser;
        this.existenceTransitionTraverser = existenceTransitionTraverser;
    }

    @Override
    public void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception {

        LOG.trace("Start:" + this.toString());
        invokeEventTraverser(batchContext, "values", valueTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "refs", refTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "multi-refs", refsTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "back-refs", allBackrefsTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "latest back-refs", latestBackrefTraverser, writtenEvent);

        invokeEventTraverser(batchContext, "ref removals", refRemovalTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "multi-ref removals", refsRemovalTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "back-ref removals", allBackRefsRemovalTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "count removals", countsRemovalTraverser, writtenEvent);
        invokeEventTraverser(batchContext, "latest back-ref removals", latestBackRefRemovalTraverser, writtenEvent);

        if (batchContext.getTransitioning().contains(writtenEvent.getWrittenInstance().getInstanceId())) {
            //invokeEventTraverser(batchContext, "existence transition", existenceTransitionTraverser, writtenEvent);
        }

        LOG.trace("End:" + this.toString());
    }

    private void invokeEventTraverser(WrittenEventContext batchContext,
            String processorName,
            EventProcessor eventProcessor,
            WrittenEvent writtenEvent) throws Exception {
        LOG.startTimer(processorName);
        try {
            eventProcessor.process(batchContext, writtenEvent);
        } finally {
            LOG.stopTimer(processorName);
        }
    }

    @Override
    public String toString() {
        return "InitiateTraversal{" + "className=" + className + '}';
    }
}

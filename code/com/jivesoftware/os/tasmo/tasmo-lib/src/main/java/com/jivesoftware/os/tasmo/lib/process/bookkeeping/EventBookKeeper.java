/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.bookkeeping;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public class EventBookKeeper implements WrittenEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final WrittenEventProcessor processorizer;

    public EventBookKeeper(WrittenEventProcessor processorizer) {
        this.processorizer = processorizer;
    }

    @Override
    public void process(WrittenEventContext batchContext,
            TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEvent writtenEvent,
            long threadTimestamp) throws Exception {

        ObjectId instanceId = writtenEvent.getWrittenInstance().getInstanceId();
        String instanceClass = instanceId.getClassName();

        try {
            LOG.startTimer("processed>" + instanceClass);
            LOG.startTimer("processed");

            if (LOG.isTraceEnabled()) {
                LOG.trace("PROCESSING TIME:{} {} EVENT-ID:{} EVENT:{}",
                        new Object[]{
                            threadTimestamp,
                            ((writtenEvent.getWrittenInstance().isDeletion()) ? "DELETE" : "UPDATE"),
                            writtenEvent.getEventId(),
                            writtenEvent});
            }

            processorizer.process(batchContext, globalCentricId, userCentricId, writtenEvent, threadTimestamp);

            LOG.inc("processed>" + instanceClass);
            LOG.inc("processed");
        } finally {
            LOG.stopTimer("processed>" + instanceClass);
            LOG.startTimer("processed");
        }
    }
}

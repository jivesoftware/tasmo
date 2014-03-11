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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public class EventBookKeeper implements EventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final EventProcessor processorizer;

    public EventBookKeeper(EventProcessor processorizer) {
        this.processorizer = processorizer;
    }

    @Override
    public boolean process(WrittenEvent writtenEvent) throws Exception {

        ObjectId instanceId = writtenEvent.getWrittenInstance().getInstanceId();
        String instanceClass = instanceId.getClassName();

        try {
            LOG.startTimer("processed>" + instanceClass);
            LOG.startTimer("processed");

            if (LOG.isTraceEnabled()) {
                LOG.trace("************* Processing: " + writtenEvent + " ******************");
            }


            boolean processed = processorizer.process(writtenEvent);

            LOG.inc("processed>" + instanceClass);
            LOG.inc("processed");

            LOG.info("Processed event :{} Instance:{} {}", new Object[]{
                writtenEvent.getEventId(),
                instanceClass,
                instanceId.getId()});

            return processed;
        } finally {
            LOG.stopTimer("processed>" + instanceClass);
            LOG.startTimer("processed");
        }
    }
}

/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.NoOpEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final DispatcherProvider dispatcherProvider;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;

    public TasmoViewMaterializer(TasmoEventBookkeeper tasmoEventBookkeeper,
        DispatcherProvider dispatcherProvider,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor) {
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.dispatcherProvider = dispatcherProvider;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            for (WrittenEvent writtenEvent : writtenEvents) {
                if (writtenEvent == null) {
                    LOG.warn("some one is sending null events.");
                    processed.add(null);
                    continue;
                }

                EventProcessor processorizer = dispatcherProvider.getDispatcher(writtenEvent.getTenantId(),
                    writtenEvent.getWrittenInstance().getInstanceId().getClassName());
                if (processorizer == null) {
                    processorizer = new NoOpEventProcessor();
                }
                if (new EventBookKeeper(processorizer).process(writtenEvent)) {
                    processed.add(writtenEvent);
                }
                //viewChangeNotificationProcessor.process(writtenEvent); //TODO - ruh roh!
            }
            tasmoEventBookkeeper.succeeded();

        } catch (Exception ex) {

            try {
                tasmoEventBookkeeper.failed();
            } catch (Exception notificationException) {
                LOG.error("Failed to notify event bookKeeper of exception: " + ex + " due to exception: " + notificationException);
            }

            throw ex;
        } finally {
            long elapse = LOG.stopTimer("processWrittenEvents");
            LOG.debug("Processed: " + writtenEvents.size() + " events in " + elapse + "millis.");
        }
        return processed;
    }
}

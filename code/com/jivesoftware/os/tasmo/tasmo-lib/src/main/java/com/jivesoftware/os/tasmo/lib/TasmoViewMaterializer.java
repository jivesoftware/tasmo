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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceUpdate;
import com.jivesoftware.os.tasmo.lib.process.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.EventProcessor;
import com.jivesoftware.os.tasmo.lib.process.NoOpEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.ArrayList;
import java.util.List;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final ExistenceStore existenceStore;
    private final DispatcherProvider dispatcherProvider;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;

    public TasmoViewMaterializer(TasmoEventBookkeeper tasmoEventBookkeeper,
        DispatcherProvider dispatcherProvider,
        ExistenceStore existenceStore,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor) {
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.existenceStore = existenceStore;
        this.dispatcherProvider = dispatcherProvider;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            List<ExistenceUpdate> exist = new ArrayList<>();
            List<ExistenceUpdate> noLongerExist = new ArrayList<>();

            for (WrittenEvent writtenEvent : writtenEvents) {
                if (writtenEvent == null) {
                    LOG.warn("some one is sending null events.");
                    processed.add(null);
                    continue;
                }

                TenantId tenantId = writtenEvent.getTenantId();
                long timestamp = writtenEvent.getEventId();
                WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                ObjectId objectId = writtenInstance.getInstanceId();
                if (writtenInstance.isDeletion()) {
                    noLongerExist.add(new ExistenceUpdate(tenantId, timestamp, objectId));
                } else {
                    exist.add(new ExistenceUpdate(tenantId, timestamp, objectId));
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

            if (!exist.isEmpty()) {
                existenceStore.addObjectId(exist);
            }
            if (!noLongerExist.isEmpty()) {
                existenceStore.removeObjectId(noLongerExist);
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

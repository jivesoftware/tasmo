/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.exists.ExistanceUpdate;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.lib.process.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.EventProcessorDispatcher;
import com.jivesoftware.os.tasmo.lib.process.NoOpEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.model.process.InMemoryModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ExistenceStore existenceStore;
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final TasmoViewModel tasmoViewModel;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;

    public TasmoViewMaterializer(ExistenceStore existenceStore, TasmoEventBookkeeper tasmoEventBookkeeper,
            TasmoViewModel tasmoViewModel,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor
    ) {
        this.existenceStore = existenceStore;
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.tasmoViewModel = tasmoViewModel;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            List<ExistanceUpdate> exist = new ArrayList<>();
            List<ExistanceUpdate> noLongerExist = new ArrayList<>();
            for (WrittenEvent writtenEvent : writtenEvents) {
                TenantId tenantId = writtenEvent.getTenantId();
                long timestamp = writtenEvent.getEventId();
                WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                ObjectId objectId = writtenInstance.getInstanceId();
                if (writtenInstance.isDeletion()) {
                    noLongerExist.add(new ExistanceUpdate(tenantId, timestamp, objectId));
                } else {
                    exist.add(new ExistanceUpdate(tenantId, timestamp, objectId));
                }
            }

            List<ExistanceUpdate> checkIfExists = new ArrayList<>();
            checkIfExists.addAll(exist);
            checkIfExists.addAll(noLongerExist);
            Set<ObjectId> currentlyExists = existenceStore.getExistence(checkIfExists);
            Set<ObjectId> transitioning = new HashSet<>();
            for (ExistanceUpdate existanceUpdate : exist) {
                if (!currentlyExists.contains(existanceUpdate.objectId)) {
                    transitioning.add(existanceUpdate.objectId);
                }
            }

            for (ExistanceUpdate existanceUpdate : noLongerExist) {
                if (currentlyExists.contains(existanceUpdate.objectId)) {
                    transitioning.add(existanceUpdate.objectId);
                }
            }

            if (!exist.isEmpty()) {
                existenceStore.addObjectId(exist);
            }
            if (!noLongerExist.isEmpty()) {
                existenceStore.removeObjectId(noLongerExist);
            }

            for (WrittenEvent writtenEvent : writtenEvents) {
                WrittenEventContext batchContext = new WrittenEventContext(new InMemoryModifiedViewProvider(), transitioning);

                if (writtenEvent == null) {
                    LOG.warn("some one is sending null events.");
                    processed.add(null);
                    continue;
                }
                VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(writtenEvent.getTenantId());
                if (model == null) {
                    LOG.error("Cannot process an event until a model has been loaded.");
                    throw new Exception("Cannot process an event until a model has been loaded.");
                } else {
                    String className = writtenEvent.getWrittenInstance().getInstanceId().getClassName();
                    ListMultimap<String, EventProcessorDispatcher> dispatchers = model.getDispatchers();
                    for (EventProcessorDispatcher dispatcher : dispatchers.get(className)) {
                        WrittenEventProcessor processorizer = dispatcher;
                        if (processorizer == null) {
                            processorizer = new NoOpEventProcessor();
                        }
                        if (new EventBookKeeper(processorizer).process(batchContext, writtenEvent)) {
                            processed.add(writtenEvent);
                        }
                    }
                    viewChangeNotificationProcessor.process(batchContext, writtenEvent);
                }
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

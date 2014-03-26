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
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel.FieldNameAndType;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.EventBookKeeper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.lib.process.existence.ExistenceStore;
import com.jivesoftware.os.tasmo.lib.process.existence.ExistenceUpdate;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.InMemoryModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ExistenceStore existenceStore;
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final TasmoViewModel tasmoViewModel;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ConcurrencyStore concurrencyStore;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;

    public TasmoViewMaterializer(ExistenceStore existenceStore, TasmoEventBookkeeper tasmoEventBookkeeper,
            TasmoViewModel tasmoViewModel,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            WrittenInstanceHelper writtenInstanceHelper,
            ConcurrencyStore concurrencyStore,
            EventValueStore eventValueStore,
            ReferenceStore referenceStore
    ) {
        this.existenceStore = existenceStore;
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.tasmoViewModel = tasmoViewModel;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.concurrencyStore = concurrencyStore;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            List<ExistenceUpdate> exist = new ArrayList<>();
            List<ExistenceUpdate> noLongerExist = new ArrayList<>();
            for (WrittenEvent writtenEvent : writtenEvents) {
                TenantId tenantId = writtenEvent.getTenantId();
                long timestamp = writtenEvent.getEventId();
                WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                ObjectId objectId = writtenInstance.getInstanceId();
                if (writtenInstance.isDeletion()) {
                    noLongerExist.add(new ExistenceUpdate(tenantId, timestamp, objectId));
                } else {
                    exist.add(new ExistenceUpdate(tenantId, timestamp, objectId));
                }
            }

            if (!exist.isEmpty()) {
                existenceStore.addObjectId(exist);
            }
            if (!noLongerExist.isEmpty()) {
                existenceStore.removeObjectId(noLongerExist);
            }

            for (WrittenEvent writtenEvent : writtenEvents) {
                WrittenEventContext batchContext = new WrittenEventContext(new InMemoryModifiedViewProvider());

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

                    // Bad Hacky! Begin
                    WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                    long timestamp = writtenEvent.getEventId();
                    TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(writtenEvent.getTenantId(),
                            Id.NULL); // TODO handle centric (idCentric) ? writtenEvent.getCentricId() :
                    ObjectId instanceId = writtenInstance.getInstanceId();

                    if (writtenInstance.isDeletion()) {
                        ListMultimap<String, FieldNameAndType> eventModel = model.getEventModel();
                        Set<String> fieldNames = new HashSet<>();
                        for (FieldNameAndType fieldNameAndType : eventModel.get(className)) {
                            if (fieldNameAndType.getFieldType() == ModelPathStepType.value) {
                                String fieldName = fieldNameAndType.getFieldName();
                                fieldNames.add(fieldName);
                            }
                        }
                        if (!fieldNames.isEmpty()) {
                            eventValueStore.removeObjectId(tenantIdAndCentricId, timestamp, instanceId, fieldNames.toArray(new String[fieldNames.size()]));
                        }
                    } else {

                        EventValueStore.Transaction transaction = eventValueStore.begin(tenantIdAndCentricId,
                                timestamp,
                                timestamp,
                                instanceId);

                        ListMultimap<String, FieldNameAndType> eventModel = model.getEventModel();
                        for (FieldNameAndType fieldNameAndType : eventModel.get(className)) {
                            String fieldName = fieldNameAndType.getFieldName();
                            if (writtenInstance.hasField(fieldName)) {
                                if (fieldNameAndType.getFieldType() == ModelPathStepType.ref) {
                                    long highest = concurrencyStore.highest(tenantIdAndCentricId.getTenantId(), instanceId, fieldName, timestamp);
                                    if (timestamp >= highest) {
                                        Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName);
                                        referenceStore.link(tenantIdAndCentricId, timestamp, instanceId, fieldName, tos);
                                    }
                                } else {
                                    OpaqueFieldValue got = writtenInstance.getFieldValue(fieldName);
                                    if (got == null || got.isNull()) {
                                        transaction.remove(fieldName);
                                    } else {
                                        transaction.set(fieldName, got);
                                    }
                                }
                            }
                        }
                        eventValueStore.commit(transaction);
                    }

                    // Bad Hacky! End
                    ListMultimap<String, InitiateTraversal> dispatchers = model.getDispatchers();
                    for (InitiateTraversal initiateTraversal : dispatchers.get(className)) {
                        if (initiateTraversal == null) {
                            LOG.warn("No traversal defined for className:{}", className);
                            continue;
                        }

                        int attempts = 0;
                        int maxAttempts = 10;
                        while (attempts < maxAttempts) {
                            attempts++;
                            if (attempts > 1) {
                                LOG.info("attempts " + attempts);
                            }
                            try {
                                EventBookKeeper eventBookKeeper = new EventBookKeeper(initiateTraversal);
                                eventBookKeeper.process(batchContext, writtenEvent);
                                processed.add(writtenEvent);
                                break;
                            } catch (Exception e) {
                                boolean pathModifiedException = false;
                                Throwable t = e;
                                while (t != null) {
                                    if (t instanceof PathModifiedOutFromUnderneathMeException) {
                                        pathModifiedException = true;
                                        LOG.trace("** RETRY ** " + t.toString());
                                    }
                                    t = t.getCause();
                                }
                                if (pathModifiedException) {
                                    Thread.sleep(1); // TODO is yield a better choice?
                                } else {
                                    throw e;
                                }
                            }
                        }
                        if (attempts >= maxAttempts) {
                            throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts.");
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

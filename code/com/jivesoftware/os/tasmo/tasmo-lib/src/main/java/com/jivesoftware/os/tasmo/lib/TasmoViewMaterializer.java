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
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel.FieldNameAndType;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessor;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventProcessorDecorator;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
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
import com.jivesoftware.os.tasmo.reference.lib.concur.ExistenceUpdate;
import com.jivesoftware.os.tasmo.reference.lib.concur.PathModifiedOutFromUnderneathMeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final WrittenEventProcessorDecorator writtenEventProcessorDecorator;
    private final TasmoViewModel tasmoViewModel;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ConcurrencyStore concurrencyStore;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;
    private final OrderIdProvider threadTime;
    private final StripingLocksProvider<ObjectId> instanceIdLocks = new StripingLocksProvider<>(1024);

    public TasmoViewMaterializer(TasmoEventBookkeeper tasmoEventBookkeeper,
            WrittenEventProcessorDecorator writtenEventProcessorDecorator,
            TasmoViewModel tasmoViewModel,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            WrittenInstanceHelper writtenInstanceHelper,
            ConcurrencyStore concurrencyStore,
            EventValueStore eventValueStore,
            ReferenceStore referenceStore,
            OrderIdProvider threadTime
    ) {
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.writtenEventProcessorDecorator = writtenEventProcessorDecorator;
        this.tasmoViewModel = tasmoViewModel;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.concurrencyStore = concurrencyStore;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
        this.threadTime = threadTime;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            for (WrittenEvent writtenEvent : writtenEvents) {
                WrittenEventContext batchContext = new WrittenEventContext(new InMemoryModifiedViewProvider());
                if (writtenEvent == null) {
                    LOG.warn("some one is sending null events.");
                    processed.add(null);
                    continue;
                }
                TenantId tenantId = writtenEvent.getTenantId();
                VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(tenantId);
                if (model == null) {
                    LOG.error("Cannot process an event until a model has been loaded.");
                    throw new Exception("Cannot process an event until a model has been loaded.");
                } else {
                    WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                    String className = writtenInstance.getInstanceId().getClassName();

                    List<TenantIdAndCentricId> tenantIdAndCentricIds = buildTenantIdAndCentricIds(model, className, tenantId, writtenEvent);
                    ObjectId instanceId = writtenInstance.getInstanceId();
                    long timestamp = writtenEvent.getEventId();

                    synchronized (instanceIdLocks.lock(writtenInstance.getInstanceId())) {

                        for (TenantIdAndCentricId tenantIdAndCentricId : tenantIdAndCentricIds) {
                            if (writtenInstance.isDeletion()) {
                                concurrencyStore.removeObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));
                                removeValueFields(model, className, tenantIdAndCentricId, timestamp, instanceId);
                            } else {
                                concurrencyStore.addObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));
                                updateValueFields(tenantIdAndCentricId, timestamp, instanceId, model, className, writtenInstance);
                            }

                            ListMultimap<String, InitiateTraversal> dispatchers = model.getDispatchers();
                            for (InitiateTraversal initiateTraversal : dispatchers.get(className)) {
                                if (initiateTraversal == null) {
                                    LOG.warn("No traversal defined for className:{}", className);
                                    continue;
                                }
                                traverseEvent(initiateTraversal, batchContext, tenantIdAndCentricId, writtenEvent, processed);
                            }
                            viewChangeNotificationProcessor.process(batchContext, writtenEvent);
                        }
                    }
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

    private List<TenantIdAndCentricId> buildTenantIdAndCentricIds(VersionedTasmoViewModel model,
            String className,
            TenantId tenantId,
            WrittenEvent writtenEvent) {

        List<TenantIdAndCentricId> tenantIdAndCentricIds = new ArrayList<>();
        tenantIdAndCentricIds.add(new TenantIdAndCentricId(tenantId, Id.NULL));
        for (FieldNameAndType fieldNameAndType : model.getEventModel().get(className)) {
            if (fieldNameAndType.isIdCentric()) {
                tenantIdAndCentricIds.add(new TenantIdAndCentricId(tenantId, writtenEvent.getCentricId()));
                break;
            }
        }
        return tenantIdAndCentricIds;
    }

    private void traverseEvent(InitiateTraversal initiateTraversal,
            WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEvent writtenEvent,
            List<WrittenEvent> processed) throws RuntimeException, Exception {

        int attempts = 0;
        int maxAttempts = 10;
        while (attempts < maxAttempts) {
            attempts++;
            if (attempts > 1) {
                LOG.info("attempts " + attempts);
            }
            try {

                WrittenEventProcessor writtenEventProcessor = writtenEventProcessorDecorator.decorateWrittenEventProcessor(initiateTraversal);
                writtenEventProcessor.process(batchContext, tenantIdAndCentricId, writtenEvent, threadTime.nextId());
                processed.add(writtenEvent);
                break;
            } catch (Exception e) {
                boolean pathModifiedException = false;
                Throwable t = e;
                while (t != null) {
                    if (t instanceof PathModifiedOutFromUnderneathMeException) {
                        pathModifiedException = true;
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("** RETRY ** " + t.toString(), t);
                        }

                    }
                    t = t.getCause();
                }
                if (pathModifiedException) {
                    Thread.sleep(100); // TODO is yield a better choice?
                } else {
                    throw e;
                }
            }
        }
        if (attempts >= maxAttempts) {
            throw new RuntimeException("Failed to reach stasis after " + maxAttempts + " attempts.");
        }
    }

    private void removeValueFields(VersionedTasmoViewModel model,
            String className,
            TenantIdAndCentricId tenantIdAndCentricId,
            long timestamp,
            ObjectId instanceId) {

        ListMultimap<String, FieldNameAndType> eventModel = model.getEventModel();
        Set<String> fieldNames = new HashSet<>();
        for (FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            if (fieldNameAndType.getFieldType() == ModelPathStepType.value) {
                String fieldName = fieldNameAndType.getFieldName();
                fieldNames.add(fieldName);
            }
        }
        eventValueStore.removeObjectId(tenantIdAndCentricId, timestamp, instanceId, fieldNames.toArray(new String[fieldNames.size()]));
    }

    private void updateValueFields(TenantIdAndCentricId tenantIdAndCentricId,
            long timestamp,
            ObjectId instanceId,
            VersionedTasmoViewModel model,
            String className,
            WrittenInstance writtenInstance) throws Exception {

        EventValueStore.Transaction transaction = eventValueStore.begin(tenantIdAndCentricId,
                timestamp,
                timestamp,
                instanceId);

        ListMultimap<String, FieldNameAndType> eventModel = model.getEventModel();
        for (FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            String fieldName = fieldNameAndType.getFieldName();
            if (writtenInstance.hasField(fieldName)) {
                if (fieldNameAndType.getFieldType() == ModelPathStepType.ref) {
                    long highest = concurrencyStore.highest(tenantIdAndCentricId, instanceId, fieldName, timestamp);
                    if (timestamp >= highest) {
                        OpaqueFieldValue fieldValue = writtenInstance.getFieldValue(fieldName);
                        if (fieldValue.isNull()) {
                            referenceStore.link(tenantIdAndCentricId, timestamp, instanceId, fieldName, Collections.EMPTY_LIST);
                        } else {
                            Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName);
                            referenceStore.link(tenantIdAndCentricId, timestamp, instanceId, fieldName, tos);
                        }
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
}
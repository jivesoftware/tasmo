package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class TasmoEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoViewModel tasmoViewModel;
    private final StripingLocksProvider<ObjectId> instanceIdLocks = new StripingLocksProvider<>(1024);
    private final ConcurrencyStore concurrencyStore;
    private final TasmoRetryingEventTraverser eventTraverser;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private final WrittenInstanceHelper writtenInstanceHelper;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;

    public TasmoEventProcessor(TasmoViewModel tasmoViewModel,
            ConcurrencyStore concurrencyStore,
            TasmoRetryingEventTraverser eventTraverser,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            WrittenInstanceHelper writtenInstanceHelper,
            EventValueStore eventValueStore,
            ReferenceStore referenceStore) {
        this.tasmoViewModel = tasmoViewModel;
        this.concurrencyStore = concurrencyStore;
        this.eventTraverser = eventTraverser;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
    }

    public void processWrittenEvent(WrittenEvent writtenEvent) throws Exception {
        WrittenEventContext batchContext = new WrittenEventContext(new InMemoryModifiedViewProvider());

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
                        eventTraverser.traverseEvent(initiateTraversal, batchContext, tenantIdAndCentricId, writtenEvent);
                    }
                    viewChangeNotificationProcessor.process(batchContext, writtenEvent);
                }
            }
        }
    }

    private List<TenantIdAndCentricId> buildTenantIdAndCentricIds(VersionedTasmoViewModel model,
            String className,
            TenantId tenantId,
            WrittenEvent writtenEvent) {

        List<TenantIdAndCentricId> tenantIdAndCentricIds = new ArrayList<>();
        tenantIdAndCentricIds.add(new TenantIdAndCentricId(tenantId, Id.NULL));
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : model.getEventModel().get(className)) {
            if (fieldNameAndType.isIdCentric()) {
                tenantIdAndCentricIds.add(new TenantIdAndCentricId(tenantId, writtenEvent.getCentricId()));
                break;
            }
        }
        return tenantIdAndCentricIds;
    }

    private void removeValueFields(VersionedTasmoViewModel model,
            String className,
            TenantIdAndCentricId tenantIdAndCentricId,
            long timestamp,
            ObjectId instanceId) {

        ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();
        Set<String> fieldNames = new HashSet<>();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
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

        ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
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

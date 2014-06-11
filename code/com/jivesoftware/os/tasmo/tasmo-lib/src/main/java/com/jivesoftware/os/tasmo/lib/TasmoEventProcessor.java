package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.read.EventValueStoreFieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.InMemoryModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore.LinkTo;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ExistenceUpdate;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class TasmoEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoViewModel tasmoViewModel;
    private final WrittenEventProvider writtenEventProvider;
    private final ConcurrencyStore concurrencyStore;
    private final TasmoRetryingEventTraverser eventTraverser;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private final WrittenInstanceHelper writtenInstanceHelper;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;
    private final FieldValueReader fieldValueReader;
    private final ReferenceTraverser referenceTraverser;
    private final CommitChange commitChange;
    private final TasmoEdgeReport tasmoEdgeReport;
    private final TasmoProcessingStats processingStats;

    public TasmoEventProcessor(TasmoViewModel tasmoViewModel,
        WrittenEventProvider writtenEventProvider,
        ConcurrencyStore concurrencyStore,
        TasmoRetryingEventTraverser eventTraverser,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor,
        WrittenInstanceHelper writtenInstanceHelper,
        EventValueStore eventValueStore,
        ReferenceTraverser referenceTraverser,
        ReferenceStore referenceStore,
        final CommitChange delegateCommitChange,
        TasmoEdgeReport tasmoEdgeReport) {
        this.tasmoViewModel = tasmoViewModel;
        this.writtenEventProvider = writtenEventProvider;
        this.concurrencyStore = concurrencyStore;
        this.eventTraverser = eventTraverser;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
        this.processingStats = new TasmoProcessingStats();

        final EventValueStoreFieldValueReader eventValueStoreFieldValueReader = new EventValueStoreFieldValueReader(eventValueStore);
        this.fieldValueReader = new FieldValueReader() {

            @Override
            public ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues(TenantIdAndCentricId tenantIdAndCentricId,
                ObjectId objectInstanceId,
                String[] fieldNamesArray) {
                long start = System.currentTimeMillis();
                ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues = eventValueStoreFieldValueReader
                    .readFieldValues(tenantIdAndCentricId, objectInstanceId, fieldNamesArray);

                String key = "fieldsFrom:" + objectInstanceId.getClassName();
                processingStats.latency("READ FIELDS", key, System.currentTimeMillis() - start);
                return readFieldValues;
            }
        };

        this.referenceTraverser = referenceTraverser;

        this.commitChange = new CommitChange() {

            @Override
            public void commitChange(WrittenEventContext writtenEventContext,
                TenantIdAndCentricId tenantIdAndCentricId,
                List<ViewFieldChange> changes) throws CommitChangeException {

                delegateCommitChange.commitChange(writtenEventContext, tenantIdAndCentricId, changes);

            }
        };
        this.tasmoEdgeReport = tasmoEdgeReport;
    }

    public void logStats() {
        processingStats.logStats();
    }

    public void processWrittenEvent(Object lock, WrittenEvent writtenEvent) throws Exception {
        TenantId tenantId = writtenEvent.getTenantId();
        final VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(tenantId);
        if (model == null) {
            LOG.error("Cannot process an event until a model has been loaded.");
            throw new Exception("Cannot process an event until a model has been loaded.");
        }

        final ModifiedViewProvider modifiedViewProvider = new InMemoryModifiedViewProvider();
        CommitChange commitChangeNotifier = new CommitChange() {
            @Override
            public void commitChange(WrittenEventContext context,
                TenantIdAndCentricId tenantIdAndCentricId,
                List<ViewFieldChange> changes) throws CommitChangeException {

                commitChange.commitChange(context, tenantIdAndCentricId, changes);
                for (ViewFieldChange viewFieldChange : changes) {
                    if (model.getNotifiableViews().contains(viewFieldChange.getViewObjectId().getClassName())) {
                        ModifiedViewInfo modifiedViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, viewFieldChange.getViewObjectId());
                        modifiedViewProvider.add(modifiedViewInfo);
                    }
                }
            }
        };

        long startProcessingEvent = System.currentTimeMillis();
        WrittenEventContext batchContext = new WrittenEventContext(writtenEvent, writtenEventProvider,
            fieldValueReader, referenceTraverser, modifiedViewProvider, commitChangeNotifier, tasmoEdgeReport, processingStats);

        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        String className = writtenInstance.getInstanceId().getClassName();

        List<TenantIdAndCentricId> tenantIdAndCentricIds = buildTenantIdAndCentricIds(model, className, tenantId, writtenEvent);
        ObjectId instanceId = writtenInstance.getInstanceId();
        long timestamp = writtenEvent.getEventId();

        for (TenantIdAndCentricId tenantIdAndCentricId : tenantIdAndCentricIds) {
            synchronized (lock) {
                long start = System.currentTimeMillis();
                if (writtenInstance.isDeletion()) {
                    concurrencyStore.removeObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));
                    removeValueFields(model, className, tenantIdAndCentricId, timestamp, instanceId);
                } else {
                    concurrencyStore.addObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));
                    updateValueFields(tenantIdAndCentricId, timestamp, instanceId, model, className, writtenInstance);
                }
                processingStats.latency("UPDATE", className, System.currentTimeMillis() - start);

                Map<String, InitiateTraversal> dispatchers = model.getDispatchers();
                InitiateTraversal initiateTraversal = dispatchers.get(className);
                if (initiateTraversal == null) {
                    LOG.warn("No traversal defined for className:{}", className);
                    continue;
                }
                eventTraverser.traverseEvent(initiateTraversal, batchContext, tenantIdAndCentricId, writtenEvent);

            }

            long start = System.currentTimeMillis();
            viewChangeNotificationProcessor.process(batchContext, writtenEvent);
            processingStats.latency("NOTIFICATION", className, System.currentTimeMillis() - start);

        }

        long elapse = System.currentTimeMillis() - startProcessingEvent;
        LOG.info("{} millis valuePaths:{} refPaths:{} backRefPaths:{} "
            + "fanDepth:{} fanBreath:{} value:{} changes:{}  DONE PROCESSING {} event:{} instance:{} tenant:{}",
            new Object[]{ elapse,
                batchContext.valuePaths,
                batchContext.refPaths,
                batchContext.backRefPaths,
                batchContext.fanDepth,
                batchContext.fanBreath,
                batchContext.readLeaves,
                batchContext.changes,
                writtenEvent.getWrittenInstance().isDeletion() ? "DELETE" : "UPDATE",
                writtenEvent.getEventId(),
                writtenEvent.getWrittenInstance().getInstanceId(),
                writtenEvent.getTenantId() });
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

        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();
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

        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();

        List<String> refFieldNames = new ArrayList<>();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            String fieldName = fieldNameAndType.getFieldName();
            if (writtenInstance.hasField(fieldName)) {
                if (fieldNameAndType.getFieldType() == ModelPathStepType.ref) {
                    refFieldNames.add(fieldName);
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

        // Always emit the nil field to signal presence
        OpaqueFieldValue nilValue = writtenEventProvider.createNilValue();
        transaction.set(ReservedFields.NIL_FIELD, nilValue);

        // 1 multiget
        List<Long> highests = concurrencyStore.highests(tenantIdAndCentricId, instanceId, refFieldNames.toArray(new String[refFieldNames.size()]));
        List<LinkTo> batchLinkTos = new ArrayList<>(refFieldNames.size());
        for (int i = 0; i < refFieldNames.size(); i++) {
            String fieldName = refFieldNames.get(i);
            if (highests == null || highests.get(i) == null || timestamp >= highests.get(i)) {
                // 4 multi puts
                OpaqueFieldValue fieldValue = writtenInstance.getFieldValue(fieldName);
                if (fieldValue.isNull()) {
                    batchLinkTos.add(new LinkTo(fieldName, Collections.<Reference>emptyList()));
                } else {
                    Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName);
                    batchLinkTos.add(new LinkTo(fieldName, tos));
                }
            }
        }
        if (!batchLinkTos.isEmpty()) {
            referenceStore.link(tenantIdAndCentricId, instanceId, timestamp, batchLinkTos);
        }
        // 3 to 6 multiputs
        eventValueStore.commit(transaction);
    }
}

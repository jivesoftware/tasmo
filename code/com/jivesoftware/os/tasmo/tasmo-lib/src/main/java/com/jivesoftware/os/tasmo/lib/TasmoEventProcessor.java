package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
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
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore.BatchLinkTo;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ExistenceUpdate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author jonathan
 */
public class TasmoEventProcessor {

    private static final int STATS_WINDOW = 1000;
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
    private final CommitChange commitChange;
    private final TasmoEdgeReport tasmoEdgeReport;

    private final Map<String, DescriptiveStatistics> updateStats = new ConcurrentHashMap<>();
    private final Map<String, DescriptiveStatistics> traversalStats = new ConcurrentHashMap<>();
    private final Map<String, DescriptiveStatistics> fieldReadsStats = new ConcurrentHashMap<>();
    private final Map<String, DescriptiveStatistics> commitStats = new ConcurrentHashMap<>();
    private final Map<String, DescriptiveStatistics> notificationStats = new ConcurrentHashMap<>();

    public TasmoEventProcessor(TasmoViewModel tasmoViewModel,
            WrittenEventProvider writtenEventProvider,
            ConcurrencyStore concurrencyStore,
            TasmoRetryingEventTraverser eventTraverser,
            ViewChangeNotificationProcessor viewChangeNotificationProcessor,
            WrittenInstanceHelper writtenInstanceHelper,
            EventValueStore eventValueStore,
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
                DescriptiveStatistics stats = fieldReadsStats.get(key);
                if (stats == null) {
                    stats = new DescriptiveStatistics(STATS_WINDOW);
                    fieldReadsStats.put(key, stats);
                }
                stats.addValue(System.currentTimeMillis() - start);
                return readFieldValues;
            }
        };

        this.commitChange = new CommitChange() {

            @Override
            public void commitChange(WrittenEventContext writtenEventContext,
                    TenantIdAndCentricId tenantIdAndCentricId,
                    List<ViewFieldChange> changes) throws CommitChangeException {

                long start = System.currentTimeMillis();
                delegateCommitChange.commitChange(writtenEventContext, tenantIdAndCentricId, changes);
                long elapse = System.currentTimeMillis() - start;
                String writtenEventClassName = writtenEventContext.getEvent().getWrittenInstance().getInstanceId().getClassName();
                Set<String> viewKeys = new HashSet<>();
                for (ViewFieldChange c : changes) {
                    viewKeys.add(c.getViewObjectId().getClassName()); // + "." + c.getModelPathId());
                }
                for (String viewKey : viewKeys) {
                    String commitKey = "event:" + writtenEventClassName + "." + viewKey;
                    DescriptiveStatistics stats = commitStats.get(commitKey);
                    if (stats == null) {
                        stats = new DescriptiveStatistics(STATS_WINDOW);
                        commitStats.put(commitKey, stats);
                    }
                    stats.addValue(elapse);
                }
            }
        };
        this.tasmoEdgeReport = tasmoEdgeReport;
    }

    public void logStats() {
        List<SortableStat> stats = new ArrayList<>();
        for (Entry<String, DescriptiveStatistics> entrySet : updateStats.entrySet()) {
            double sla = 100;
            logStats(sla, "STATS UPDATES OF " + entrySet.getKey(), entrySet.getValue(), "millis", stats);
        }
        for (Entry<String, DescriptiveStatistics> entrySet : traversalStats.entrySet()) {
            double sla = 500;
            logStats(sla, "STATS TRAVERSAL OF " + entrySet.getKey(), entrySet.getValue(), "millis", stats);
        }
        for (Entry<String, DescriptiveStatistics> entrySet : commitStats.entrySet()) {
            double sla = 100;
            logStats(sla, "STATS COMMITS OF " + entrySet.getKey(), entrySet.getValue(), "millis", stats);
        }
        for (Entry<String, DescriptiveStatistics> entrySet : fieldReadsStats.entrySet()) {
            double sla = 100;
            logStats(sla, "STATS FIELD READS OF " + entrySet.getKey(), entrySet.getValue(), "millis", stats);
        }
        for (Entry<String, DescriptiveStatistics> entrySet : notificationStats.entrySet()) {
            double sla = 100;
            logStats(sla, "STATS NOTIFICATIONS OF " + entrySet.getKey(), entrySet.getValue(), "millis", stats);
        }
        Collections.sort(stats);
        for (SortableStat stat : stats) {
            LOG.info(stat.value + " " + stat.name);
        }
    }

    private void logStats(double sla, String name, DescriptiveStatistics ds, String units, List<SortableStat> stats) {
        logStatOverSLA(ds.getMin(), sla, units + " min " + name, stats);
        logStatOverSLA(ds.getMax(), sla, units + " max " + name, stats);
        logStatOverSLA(ds.getMean(), sla, units + " mean " + name, stats);
        //logStatOverSLA(ds.getVariance(), sla,units + " variance " + name, stats);
        logStatOverSLA(ds.getPercentile(50), sla, units + " 50th " + name, stats);
        logStatOverSLA(ds.getPercentile(75), sla, units + " 75th " + name, stats);
        logStatOverSLA(ds.getPercentile(90), sla, units + " 90th " + name, stats);
        logStatOverSLA(ds.getPercentile(95), sla, units + " 95th " + name, stats);
        logStatOverSLA(ds.getPercentile(99), sla, units + " 99th " + name, stats);
    }

    private void logStatOverSLA(double value, double sla, String name, List<SortableStat> stats) {
        if (value > sla) {
            stats.add(new SortableStat(value, name));
        }
    }

    static class SortableStat implements Comparable<SortableStat> {

        private final double value;
        private final String name;

        public SortableStat(double value, String name) {
            this.value = value;
            this.name = name;
        }

        @Override
        public int compareTo(SortableStat o) {
            return Double.compare(value, o.value);
        }
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
                fieldValueReader, modifiedViewProvider, commitChangeNotifier, tasmoEdgeReport);

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
                String updateStatKey = "event:" + className;
                DescriptiveStatistics stats = updateStats.get(updateStatKey);
                if (stats == null) {
                    stats = new DescriptiveStatistics(STATS_WINDOW);
                    updateStats.put(updateStatKey, stats);
                }
                stats.addValue(System.currentTimeMillis() - start);

                ListMultimap<String, InitiateTraversal> dispatchers = model.getDispatchers();
                for (InitiateTraversal initiateTraversal : dispatchers.get(className)) {
                    if (initiateTraversal == null) {
                        LOG.warn("No traversal defined for className:{}", className);
                        continue;
                    }
                    start = System.currentTimeMillis();
                    eventTraverser.traverseEvent(initiateTraversal, batchContext, tenantIdAndCentricId, writtenEvent);
                    String traversalStatKey = "event:" + className;
                    stats = traversalStats.get(traversalStatKey);
                    if (stats == null) {
                        stats = new DescriptiveStatistics(STATS_WINDOW);
                        traversalStats.put(traversalStatKey, stats);
                    }
                    stats.addValue(System.currentTimeMillis() - start);
                }
            }

            long start = System.currentTimeMillis();
            viewChangeNotificationProcessor.process(batchContext, writtenEvent);
            String commitStatKey = "event:" + className;
            DescriptiveStatistics stats = notificationStats.get(commitStatKey);
            if (stats == null) {
                stats = new DescriptiveStatistics(STATS_WINDOW);
                notificationStats.put(commitStatKey, stats);
            }
            stats.addValue(System.currentTimeMillis() - start);

        }

        long elapse = System.currentTimeMillis() - startProcessingEvent;
        LOG.info("{} millis paths:{} fanDepth:{} fanBreath:{} value:{} changes:{}  DONE PROCESSING {} event:{} instance:{} tenant:{}", new Object[]{elapse,
            batchContext.paths,
            batchContext.fanDepth,
            batchContext.fanBreath,
            batchContext.readLeaves,
            batchContext.changes,
            writtenEvent.getWrittenInstance().isDeletion() ? "DELETE" : "UPDATE",
            writtenEvent.getEventId(),
            writtenEvent.getWrittenInstance().getInstanceId(),
            writtenEvent.getTenantId()});
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

//        String[] vfn = valueFieldNames.toArray(new String[valueFieldNames.size()]);
//        // HACK
//        ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] readFieldValues = fieldValueReader.readFieldValues(tenantIdAndCentricId, instanceId, vfn);
//        // end HACK
//        for (int i = 0; i < vfn.length; i++) {
//
//            OpaqueFieldValue update = writtenInstance.getFieldValue(vfn[i]);
//            if (update == null || update.isNull()) {
//                transaction.remove(vfn[i]);
//            } else {
//                if (readFieldValues[i] != null && readFieldValues[i].getValue() != null && readFieldValues[i].getValue().equals(update)) {
//                    writtenInstance.removeField(vfn[i]);
//                    LOG.warn("HACK remove unchagned field:" + vfn[i]);
//                }
//                transaction.set(vfn[i], update);
//            }
//        }
        // 1 multiget
        List<Long> highests = concurrencyStore.highests(tenantIdAndCentricId, instanceId, refFieldNames.toArray(new String[refFieldNames.size()]));
        List<BatchLinkTo> batchLinkTos = new ArrayList<>(refFieldNames.size());
        for (int i = 0; i < refFieldNames.size(); i++) {
            String fieldName = refFieldNames.get(i);
            if (highests == null || highests.get(i) == null || timestamp >= highests.get(i)) {
                // 4 multi puts
                OpaqueFieldValue fieldValue = writtenInstance.getFieldValue(fieldName);
                if (fieldValue.isNull()) {
                    batchLinkTos.add(new BatchLinkTo(fieldName, Collections.EMPTY_LIST));
                } else {
                    Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName);
                    batchLinkTos.add(new BatchLinkTo(fieldName, tos));
                }
            }
        }
        if (!batchLinkTos.isEmpty()) {
            referenceStore.batchLink(tenantIdAndCentricId, instanceId, timestamp, batchLinkTos);
        }
        // 3 to 6 multiputs
        eventValueStore.commit(transaction);
    }
}

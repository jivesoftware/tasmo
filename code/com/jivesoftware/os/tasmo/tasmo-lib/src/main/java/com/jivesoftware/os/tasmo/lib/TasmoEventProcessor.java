package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.InMemoryModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jonathan
 */
public class TasmoEventProcessor {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoViewModel tasmoViewModel;
    private final TasmoEventPersistor eventPersistor;
    private final WrittenEventProvider writtenEventProvider;
    private final TasmoEventTraverser eventTraverser;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;

    private final FieldValueReader fieldValueReader;
    private final ReferenceTraverser referenceTraverser;
    private final CommitChange commitChange;
    private final TasmoEdgeReport tasmoEdgeReport;
    private final TasmoProcessingStats processingStats;

    public TasmoEventProcessor(TasmoViewModel tasmoViewModel,
        TasmoEventPersistor eventPersistor,
        WrittenEventProvider writtenEventProvider,
        TasmoEventTraverser eventTraverser,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor,
        FieldValueReader fieldValueReader,
        ReferenceTraverser referenceTraverser,
        CommitChange commitChange,
        TasmoProcessingStats processingStats,
        TasmoEdgeReport tasmoEdgeReport) {

        this.tasmoViewModel = tasmoViewModel;
        this.eventPersistor = eventPersistor;
        this.writtenEventProvider = writtenEventProvider;
        this.eventTraverser = eventTraverser;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.fieldValueReader = fieldValueReader;
        this.referenceTraverser = referenceTraverser;
        this.commitChange = commitChange;
        this.processingStats = processingStats;
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
        WrittenEventContext batchContext = new WrittenEventContext(writtenEvent.getEventId(),
            writtenEvent.getActorId(), writtenEvent, writtenEventProvider,
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
                    eventPersistor.removeValueFields(model, className, tenantIdAndCentricId, timestamp, instanceId);
                } else {
                    eventPersistor.updateValueFields(tenantIdAndCentricId, timestamp, instanceId, model, className, writtenInstance);
                }
                processingStats.latency("UPDATE", className, System.currentTimeMillis() - start);

                Map<String, InitiateWriteTraversal> traverser = model.getWriteTraversers();
                InitiateWriteTraversal initiateTraversal = traverser.get(className);
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
}

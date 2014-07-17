package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.model.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.notification.ViewChangeNotificationProcessor;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.TasmoEventTraversal;
import com.jivesoftware.os.tasmo.lib.read.FieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.EventPersistor;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.process.InMemoryModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotificationListener;
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
    private final EventPersistor eventPersistor;
    private final WrittenEventProvider writtenEventProvider;
    private final TasmoEventTraversal eventTraverser;
    private final ViewChangeNotificationProcessor viewChangeNotificationProcessor;
    private final ViewNotificationListener allViewNotificationsListener;
    private final ConcurrencyStore concurrencyStore;
    private final ReferenceStore referenceStore;
    private final FieldValueReader fieldValueReader;
    private final ReferenceTraverser referenceTraverser;
    private final CommitChange commitChange;
    private final TasmoProcessingStats processingStats;

    public TasmoEventProcessor(TasmoViewModel tasmoViewModel,
        EventPersistor eventPersistor,
        WrittenEventProvider writtenEventProvider,
        TasmoEventTraversal eventTraverser,
        ViewChangeNotificationProcessor viewChangeNotificationProcessor, //Deprecate
        ViewNotificationListener allViewNotificationsListener,
        ConcurrencyStore concurrencyStore,
        ReferenceStore referenceStore,
        FieldValueReader fieldValueReader,
        ReferenceTraverser referenceTraverser,
        CommitChange commitChange,
        TasmoProcessingStats processingStats) {

        this.tasmoViewModel = tasmoViewModel;
        this.eventPersistor = eventPersistor;
        this.writtenEventProvider = writtenEventProvider;
        this.eventTraverser = eventTraverser;
        this.viewChangeNotificationProcessor = viewChangeNotificationProcessor;
        this.allViewNotificationsListener = allViewNotificationsListener;
        this.concurrencyStore = concurrencyStore;
        this.referenceStore = referenceStore;
        this.fieldValueReader = fieldValueReader;
        this.referenceTraverser = referenceTraverser;
        this.commitChange = commitChange;
        this.processingStats = processingStats;
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
                List<ViewField> changes) throws CommitChangeException {
                commitChange.commitChange(context, tenantIdAndCentricId, changes);

                List<ViewNotification> notifications = new ArrayList<>();
                for (ViewField viewFieldChange : changes) {
                    notifications.add(new ViewNotification(tenantIdAndCentricId,
                        viewFieldChange.getEventId(),
                        viewFieldChange.getActorId(),
                        viewFieldChange.getViewObjectId()));

                    // Old way factor out
                    if (model.getNotifiableViews().contains(viewFieldChange.getViewObjectId().getClassName())) {
                        ModifiedViewInfo modifiedViewInfo = new ModifiedViewInfo(tenantIdAndCentricId, viewFieldChange.getViewObjectId());
                        modifiedViewProvider.add(modifiedViewInfo);
                    }
                }
                try {
                    allViewNotificationsListener.handleNotifications(notifications);
                } catch (Exception ex) {
                    throw new CommitChangeException("Failed to send allView notifications.", ex);
                }
            }
        };

        long startProcessingEvent = System.currentTimeMillis();
        ConcurrencyChecker concurrencyChecker = new ConcurrencyChecker(concurrencyStore);
        WrittenEventContext batchContext = new WrittenEventContext(writtenEvent.getEventId(),
            writtenEvent.getActorId(), writtenEvent, writtenEventProvider, concurrencyChecker, referenceStore,
            fieldValueReader, referenceTraverser, modifiedViewProvider, commitChangeNotifier, processingStats);

        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        String className = writtenInstance.getInstanceId().getClassName();

        // Process event globally
        TenantIdAndCentricId tenantIdAndGloballyCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        process(lock, model, model.getWriteTraversers(), className, writtenInstance, tenantIdAndGloballyCentricId, batchContext, writtenEvent);

        // Process event centrically if centric is set.
        //if (!Id.NULL.equals(writtenEvent.getCentricId())) {
            TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, writtenEvent.getCentricId());
            process(lock, model, model.getCentricWriteTraversers(), className, writtenInstance, tenantIdAndCentricId, batchContext, writtenEvent);
        //}

        long start = System.currentTimeMillis();
        viewChangeNotificationProcessor.process(batchContext, writtenEvent);
        allViewNotificationsListener.flush();
        processingStats.latency("NOTIFICATION", className, System.currentTimeMillis() - start);


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

    private void process(Object lock,
        VersionedTasmoViewModel model,
        Map<String, InitiateWriteTraversal> traversers,
        String className,
        WrittenInstance writtenInstance,
        TenantIdAndCentricId tenantIdAndGloballyCentricId,
        WrittenEventContext batchContext,
        WrittenEvent writtenEvent) throws Exception {


        WrittenEventProcessor initiateTraversal = traversers.get(className);
        if (initiateTraversal != null) {
            ObjectId instanceId = writtenInstance.getInstanceId();
            long timestamp = writtenEvent.getEventId();
            long start = System.currentTimeMillis();
            synchronized (lock) {
                if (writtenInstance.isDeletion()) {
                    eventPersistor.removeValueFields(model, className, tenantIdAndGloballyCentricId, instanceId, timestamp);
                } else {
                    eventPersistor.updateValueFields(model, className, tenantIdAndGloballyCentricId, instanceId, timestamp, writtenInstance);
                }
                processingStats.latency("UPDATE", className, System.currentTimeMillis() - start);
                eventTraverser.traverseEvent(initiateTraversal, batchContext, tenantIdAndGloballyCentricId, writtenEvent);
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
}

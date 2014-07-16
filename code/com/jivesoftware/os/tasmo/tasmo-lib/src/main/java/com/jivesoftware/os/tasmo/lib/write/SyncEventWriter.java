package com.jivesoftware.os.tasmo.lib.write;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.TasmoBlacklist;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.model.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.lib.modifier.ModifierStore;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.BookkeepingEvent;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 *
 * @author jonathan.colt
 */
public class SyncEventWriter implements CallbackStream<List<WrittenEvent>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ListeningExecutorService processEvents;
    private final TasmoViewModel tasmoViewModel;
    private final EventPersistor tasmoEventPersistor;
    private final ModifierStore modifierStore;
    private final CallbackStream<List<BookkeepingEvent>> bookkeepingStream;
    private final TasmoBlacklist tasmoBlacklist;
    private final ConcurrentHashMap<TenantId, StripingLocksProvider<ObjectId>> instanceIdLocks = new ConcurrentHashMap<>();

    public SyncEventWriter(ListeningExecutorService processEvents,
        TasmoViewModel tasmoViewModel,
        EventPersistor tasmoEventPersistor,
        ModifierStore modifierStore,
        CallbackStream<List<BookkeepingEvent>> bookkeepingStream,
        TasmoBlacklist tasmoBlacklist) {

        this.processEvents = processEvents;
        this.tasmoViewModel = tasmoViewModel;
        this.tasmoEventPersistor = tasmoEventPersistor;
        this.modifierStore = modifierStore;
        this.bookkeepingStream = bookkeepingStream;
        this.tasmoBlacklist = tasmoBlacklist;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> writtenEvents) throws Exception {

        final List<WrittenEvent> failedToProcess = new ArrayList<>(writtenEvents.size());
        final List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        try {

            Multimap<Object, WrittenEvent> writtenEventLockGroups = ArrayListMultimap.create();
            for (WrittenEvent writtenEvent : writtenEvents) {
                if (writtenEvent != null) {
                    if (tasmoBlacklist.blacklisted(writtenEvent)) {
                        LOG.info("BLACKLISTED event" + writtenEvent);
                    } else {

                        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                        TenantId tenantId = writtenEvent.getTenantId();
                        StripingLocksProvider<ObjectId> tenantLocks = instanceIdLocks.get(tenantId);
                        if (tenantLocks == null) {
                            tenantLocks = new StripingLocksProvider<>(1_024); // Expose to config?
                            StripingLocksProvider<ObjectId> had = instanceIdLocks.putIfAbsent(tenantId, tenantLocks);
                            if (had != null) {
                                tenantLocks = had;
                            }
                        }
                        Object lock = tenantLocks.lock(writtenInstance.getInstanceId());
                        writtenEventLockGroups.put(lock, writtenEvent);
                    }
                }
            }

            Set<Object> locks = writtenEventLockGroups.keySet();
            final CountDownLatch batchCompletionLatch = new CountDownLatch(locks.size());
            List<Future> futures = new ArrayList<>();
            for (final Object lock : locks) {
                final Collection<WrittenEvent> events = writtenEventLockGroups.get(lock);
                Future<?> future = processEvents.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            for (WrittenEvent event : events) {
                                try {
                                    TenantId tenantId = event.getTenantId();
                                    final VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(tenantId);
                                    if (model == null) {
                                        LOG.error("Cannot process an event until a model has been loaded.");
                                        throw new RuntimeException("Cannot process an event until a model has been loaded.");
                                    }
                                    WrittenInstance writtenInstance = event.getWrittenInstance();
                                    String className = writtenInstance.getInstanceId().getClassName();
                                    TenantIdAndCentricId tenantIdAndGloballyCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
                                    TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, event.getCentricId());
                                    ObjectId instanceId = writtenInstance.getInstanceId();
                                    long timestamp = event.getEventId();

                                    synchronized (lock) {
                                        if (event.getWrittenInstance().isDeletion()) {
                                            tasmoEventPersistor.removeValueFields(model, className, tenantIdAndGloballyCentricId, instanceId, timestamp - 1);
                                            tasmoEventPersistor.removeValueFields(model, className, tenantIdAndCentricId, instanceId, timestamp - 1);
                                        } else {
                                            tasmoEventPersistor.updateValueFields(model, className, tenantIdAndGloballyCentricId, instanceId, timestamp,
                                                writtenInstance);
                                            tasmoEventPersistor.
                                                updateValueFields(model, className, tenantIdAndCentricId, instanceId, timestamp, writtenInstance);
                                        }
                                        Set<ObjectId> ids = eventToRefId(model, className, instanceId, writtenInstance);
                                        modifierStore.add(tenantId, event.getActorId(), ids, timestamp);
                                    }
                                    processed.add(event);
                                } catch (Exception x) {
                                    failedToProcess.add(event);
                                    LOG.warn("Failed to persist eventId:{} instanceId:{} tenantId:{} exception: {}", new Object[]{
                                        event.getEventId(),
                                        event.getWrittenInstance().getInstanceId(),
                                        event.getTenantId(),
                                        x
                                    }, x);
                                    if (LOG.isTraceEnabled()) {
                                        LOG.trace("Failed to persist writtenEvent:" + event, x);
                                    }
                                }
                            }
                        } finally {
                            batchCompletionLatch.countDown();
                        }
                    }
                });
                futures.add(future);
            }
            batchCompletionLatch.await();

        } catch (Exception ex) {
            throw ex;
        }

        List<BookkeepingEvent> bookkeepingEvents = new ArrayList<>();
        for (WrittenEvent p : processed) {
            if (p.isBookKeepingEnabled()) {
                bookkeepingEvents.add(new BookkeepingEvent(p.getTenantId(), p.getActorId(), p.getEventId(), true));
            }
        }
        bookkeepingStream.callback(bookkeepingEvents);

        return failedToProcess;
    }

    private Set<ObjectId> eventToRefId(VersionedTasmoViewModel model, String className, ObjectId instanceId, WrittenInstance writtenInstance) {
        HashSet<ObjectId> ids = new HashSet<>();
        ids.add(instanceId);
        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            String fieldName = fieldNameAndType.getFieldName();
            ModelPathStepType fieldType = fieldNameAndType.getFieldType();
            if (fieldType != ModelPathStepType.value && !fieldType.isBackReferenceType() && writtenInstance.hasField(fieldName)) {
                if (fieldType == ModelPathStepType.ref) {
                    ObjectId referenceFieldValue = writtenInstance.getReferenceFieldValue(fieldName);
                    if (referenceFieldValue != null) {
                        ids.add(referenceFieldValue);
                    }
                } else if (fieldType == ModelPathStepType.refs) {
                    ObjectId[] multiReferenceFieldValue = writtenInstance.getMultiReferenceFieldValue(fieldName);
                    if (multiReferenceFieldValue != null) {
                        for (ObjectId objectId : multiReferenceFieldValue) {
                            ids.add(objectId);
                        }
                    }
                }
            }
        }
        return ids;
    }

}

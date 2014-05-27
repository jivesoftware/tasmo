package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final TasmoEventProcessor eventProcessor;
    private final ListeningExecutorService processEvents;
    private final ConcurrentHashMap<TenantId, StripingLocksProvider<ObjectId>> instanceIdLocks = new ConcurrentHashMap<>();
    private final AtomicLong totalProcessed = new AtomicLong();

    public TasmoViewMaterializer(TasmoEventBookkeeper tasmoEventBookkeeper,
            TasmoEventProcessor eventProcessor,
            ListeningExecutorService processEvents
    ) {
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.eventProcessor = eventProcessor;
        this.processEvents = processEvents;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {

        final List<WrittenEvent> processed = new ArrayList<>(writtenEvents.size());
        final List<WrittenEvent> failedToProcess = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");

            Multimap<Object, WrittenEvent> writtenEventLockGroups = ArrayListMultimap.create();
            for (WrittenEvent writtenEvent : writtenEvents) {
                if (writtenEvent != null) {
                    WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
                    TenantId tenantId = writtenEvent.getTenantId();
                    StripingLocksProvider<ObjectId> tenantLocks = instanceIdLocks.get(tenantId);
                    if (tenantLocks == null) {
                        tenantLocks = new StripingLocksProvider<>(1024); // Expose to config?
                        StripingLocksProvider<ObjectId> had = instanceIdLocks.putIfAbsent(tenantId, tenantLocks);
                        if (had != null) {
                            tenantLocks = had;
                        }
                    }
                    Object lock = tenantLocks.lock(writtenInstance.getInstanceId());
                    writtenEventLockGroups.put(lock, writtenEvent);
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
                                    eventProcessor.processWrittenEvent(lock, event);
                                    processed.add(event);
                                } catch (Exception x) {
                                    failedToProcess.add(event);
                                    LOG.warn("CONSISTENCY Failed to process eventId:{} instanceId:{} tenantId:{} exception: {}", new Object[]{
                                        event.getEventId(),
                                        event.getWrittenInstance().getInstanceId(),
                                        event.getTenantId(),
                                        x
                                    });
                                    if (LOG.isTraceEnabled()) {
                                        LOG.trace("Failed to process writtenEvent:" + event, x);
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

//            for (Future future : futures) {
//                future.get(); // progagate exceptions to caller.
//            }
            tasmoEventBookkeeper.begin(processed);
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
            if (elapse == 0) {
                elapse = 1;
            }
            totalProcessed.addAndGet(writtenEvents.size());
            double eventsPerSecond = 1000d / ((double) elapse / (double) writtenEvents.size());
            double eps = eventsPerSecond * 0.5d + lastEventsPerSecond * 0.5d;
            lastEventsPerSecond = eventsPerSecond;
            LOG.info("BATCH PROCESSED: events:{} in {} millis  totalEvents:{} currentEventPerSecond:{}",
                    new Object[]{writtenEvents.size(), elapse, totalProcessed, eps});
        }

        if (failedToProcess.size() > 0) {
            LOG.warn("CONSISTENCY please retry " + failedToProcess.size()+ " later.");
        }
        return failedToProcess;
    }
    double lastEventsPerSecond = 0;

}

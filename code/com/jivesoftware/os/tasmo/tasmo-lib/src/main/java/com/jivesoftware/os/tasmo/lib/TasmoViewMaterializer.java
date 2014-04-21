package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.bookkeeping.TasmoEventBookkeeper;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TasmoViewMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TasmoEventBookkeeper tasmoEventBookkeeper;
    private final TasmoEventProcessor eventProcessor;
    private final ExecutorService processEvents;

    public TasmoViewMaterializer(TasmoEventBookkeeper tasmoEventBookkeeper,
            TasmoEventProcessor eventProcessor,
            ExecutorService processEvents
    ) {
        this.tasmoEventBookkeeper = tasmoEventBookkeeper;
        this.eventProcessor = eventProcessor;
        this.processEvents = processEvents;
    }

    public List<WrittenEvent> process(List<WrittenEvent> writtenEvents) throws Exception {
        final List<WrittenEvent> failedToProcess = new ArrayList<>(writtenEvents.size());
        try {
            LOG.startTimer("processWrittenEvents");
            tasmoEventBookkeeper.begin(writtenEvents);

            final CountDownLatch batchCompletionLatch = new CountDownLatch(writtenEvents.size());
            List<Future> futures = new ArrayList<>();
            for (final WrittenEvent writtenEvent : writtenEvents) {
                if (writtenEvent == null) {
                    batchCompletionLatch.countDown();
                    LOG.warn("some one is sending null events.");
                } else {
                    Future<?> future = processEvents.submit(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                eventProcessor.processWrittenEvent(writtenEvent);
                            } catch (Exception x) {
                                failedToProcess.add(writtenEvent);
                                LOG.error("Failed to process writtenEvent:" + writtenEvent, x);
                            } finally {
                                batchCompletionLatch.countDown();
                            }
                        }
                    });
                    futures.add(future);
                }
            }
            batchCompletionLatch.await();

            for (Future future : futures) {
                future.get(); // progegate exceptions to caller.
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
            LOG.info("BATCH PROCESSED: elapse:{} millis events:{}", new Object[]{elapse, writtenEvents.size()});
        }
        return failedToProcess;
    }

}

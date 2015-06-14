package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.process.TasmoProcessingStats;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 * @author jonathan.colt
 */
public class TasmoProcessingStatsInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoProcessingStatsConfig extends Config {

        @IntDefault (-1)
        public Integer getLogStatsEveryNSeconds();

        public void setLogStatsEveryNSeconds(int nSeconds);
    }

    public static TasmoServiceHandle<TasmoProcessingStats> initialize(final TasmoProcessingStatsConfig config) {
        final TasmoProcessingStats processingStats = new TasmoProcessingStats();
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        return new TasmoServiceHandle<TasmoProcessingStats>() {

            @Override
            public TasmoProcessingStats getService() {
                return processingStats;
            }

            @Override
            public void start() throws Exception {
                int logStatsEveryNSeconds = config.getLogStatsEveryNSeconds();
                if (logStatsEveryNSeconds > 0) {
                    scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processingStats.logStats();
                            } catch (Exception x) {
                                LOG.error("Issue with logging stats. ", x);
                            }
                        }
                    }, logStatsEveryNSeconds, logStatsEveryNSeconds, TimeUnit.SECONDS);
                }
            }

            @Override
            public void stop() throws Exception {
                scheduledExecutorService.shutdownNow();
            }
        };
    }
}

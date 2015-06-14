package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 * @author jonathan.colt
 */
public class TasmoViewModelInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    static public interface TasmoViewModelConfig extends Config {

        @StringDefault ("master")
        public String getModelMasterTenantId();
        public void setModelMasterTenantId(String tenantId);

        @IntDefault (10)
        public Integer getPollForModelChangesEveryNSeconds();
        public void setPollForModelChangesEveryNSeconds(Integer seconds);

    }

    public static TasmoServiceHandle<TasmoViewModel> initialize(
        ViewsProvider viewsProvider,
        ViewPathKeyProvider viewPathKeyProvider,
        final TasmoViewModelConfig config) {

        final TenantId masterTenantId = new TenantId(config.getModelMasterTenantId());
        final TasmoViewModel tasmoViewModel = new TasmoViewModel(
            masterTenantId,
            viewsProvider,
            viewPathKeyProvider);
        tasmoViewModel.loadModel(masterTenantId); // Move to start method?


        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        return new TasmoServiceHandle<TasmoViewModel>() {

            @Override
            public TasmoViewModel getService() {
                return tasmoViewModel;
            }

            @Override
            public void start() throws Exception {

                scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            tasmoViewModel.reloadModels();
                        } catch (Exception x) {
                            LOG.error("Scheduled reloading of tasmo view model failed. ", x);
                        }
                    }
                }, config.getPollForModelChangesEveryNSeconds(), config.getPollForModelChangesEveryNSeconds(), TimeUnit.SECONDS);
            }

            @Override
            public void stop() throws Exception {
                scheduledExecutorService.shutdownNow();
            }
        };
    }
}

package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializer;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import org.merlin.config.Config;

/**
 *
 * @author jonathan.colt
 */
public class TasmoNotificationReadMaterializerInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface TasmoNotificationReadMaterializerConfig extends Config {
    }

    public static TasmoServiceHandle<TasmoNotificationsIngress> initialize(TasmoNotificationReadMaterializerConfig config,
        OrderIdProvider threadTimestamp,
        ReadMaterializer readMaterializer,
        ViewValueWriter viewValueWriter) throws Exception {

        final TasmoNotificationsIngress notificationsIngress = new TasmoNotificationsIngress(threadTimestamp,
            readMaterializer,
            viewValueWriter);
        return new TasmoServiceHandle<TasmoNotificationsIngress>() {

            @Override
            public TasmoNotificationsIngress getService() {
                return notificationsIngress;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
            }
        };
    }
}

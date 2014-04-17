package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.google.common.collect.Iterables;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotificationListener;
import java.util.List;

/**
 * Simple composition to receive a batch of view change notifications, load the corresponding views, apply a matcher,
 * process them according to a specific strategy, and finally invoke a handler if the view change is accepted.
 */
public class AsyncViewNotificationsListener<V extends BaseView<?>> implements ViewNotificationListener, ViewNotificationsCallback<V> {

    private static final MetricLogger logger = MetricLoggerFactory.getLogger();

    private final ViewNotificationsHandler<V> viewNotificationsHandler;
    private final ViewMatcher<V> viewMatcher;
    private final ViewProcessor<V> viewProcessor;
    private final ViewHandler<V> viewHandler;

    public AsyncViewNotificationsListener(
        ViewNotificationsHandler<V> viewNotificationsHandler,
        ViewMatcher<V> viewMatcher,
        ViewProcessor<V> viewProcessor,
        ViewHandler<V> viewHandler) {
        this.viewNotificationsHandler = viewNotificationsHandler;
        this.viewMatcher = viewMatcher;
        this.viewProcessor = viewProcessor;
        this.viewHandler = viewHandler;
    }

    @Override
    public void handleNotifications(List<ViewNotification> viewNotifications) {
        logger.trace("handleNotifications: {}", viewNotifications.size());
        try {
            viewNotificationsHandler.handleNotifications(viewNotifications, this);
        } catch (Exception e) {
            //TODO kafka should handle exceptions and retry, but it appears to enter a death spiral
            logger.error("Failed to handle notifications: {}" , viewNotifications);
            logger.error("Reason", e);
        }
    }

    @Override
    public void handleViewChanges(Iterable<ViewChange<V>> viewChanges) throws Exception {
        viewProcessor.processViews(Iterables.filter(viewChanges, viewMatcher), viewHandler);
    }
}

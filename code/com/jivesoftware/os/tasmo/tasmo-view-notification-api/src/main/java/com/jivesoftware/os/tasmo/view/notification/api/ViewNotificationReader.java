package com.jivesoftware.os.tasmo.view.notification.api;

import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;


public interface ViewNotificationReader extends ServiceHandle {

    /**
     * Returns the last view notification for a specific view class.
     *
     * @param viewClassName view class name for which notification was sent
     */
    ViewNotification getLastNotification(String viewClassName);

    /**
     * Registers a notification listener for a specific view and consumer group. Notifications can be sent in batches by providing a desired batch size
     * and timeout.
     *
     * @param viewClassName view class name to monitor
     * @param viewNotificationListener listener that implements the callback method
    */
    void registerListener(String viewClassName, ViewNotificationListener viewNotificationListener);

}

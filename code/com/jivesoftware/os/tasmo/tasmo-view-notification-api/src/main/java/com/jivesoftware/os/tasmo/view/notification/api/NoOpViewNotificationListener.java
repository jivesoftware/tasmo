package com.jivesoftware.os.tasmo.view.notification.api;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class NoOpViewNotificationListener implements ViewNotificationListener {

    @Override
    public void handleNotifications(List<ViewNotification> viewNotifications) throws Exception {
    }

    @Override
    public void flush() {
    }

}

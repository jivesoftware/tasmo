package com.jivesoftware.os.tasmo.view.notification.api;

import java.util.List;

public interface ViewNotificationListener {

    void handleNotifications(List<ViewNotification> viewNotifications) throws Exception;

    void flush();
}

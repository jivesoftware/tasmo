package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.view.notification.api.ViewNotification;
import java.util.List;

/**
 * Takes a batch of view change notifications and invokes a callback for each view.
 */
public interface ViewNotificationsHandler<V extends BaseView<?>> {

    void handleNotifications(List<ViewNotification> viewNotifications, ViewNotificationsCallback<V> callback);

}

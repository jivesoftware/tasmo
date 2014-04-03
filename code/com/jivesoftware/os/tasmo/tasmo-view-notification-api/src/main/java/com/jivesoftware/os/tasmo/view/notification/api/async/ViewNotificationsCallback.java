package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;
import com.jivesoftware.os.tasmo.id.BaseView;


/**
 *
 */
public interface ViewNotificationsCallback<V extends BaseView<?>> {

    void handleViewChanges(Iterable<ViewChange<V>> viewChanges) throws Exception;
}

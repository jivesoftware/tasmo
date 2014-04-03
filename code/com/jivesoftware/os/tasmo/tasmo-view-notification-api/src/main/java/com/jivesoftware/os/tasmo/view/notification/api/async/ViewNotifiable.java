package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.id.BaseView;

/**
 *
 */
public interface ViewNotifiable<V extends BaseView<?>> {

    Class<V> getNotifiableViewClass();
}

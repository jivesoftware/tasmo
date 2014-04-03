package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;
import com.jivesoftware.os.tasmo.id.BaseView;

/**
 * Handles a view change. This is where side effects finally occur, e.g. writing new events or committing state change.
 */
public interface ViewHandler<V extends BaseView<?>> extends ViewNotifiable<V> {

    void handleView(ViewChange<V> viewChange);
}

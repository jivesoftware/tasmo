package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;
import com.jivesoftware.os.tasmo.id.BaseView;

/**
 * Processes views according to some strategy (e.g. once-when-matched) and, if acceptable, delivers to the handler.
 */
public interface ViewProcessor<V extends BaseView<?>> {

    void processViews(Iterable<ViewChange<V>> viewChanges, ViewHandler<V> handler) throws Exception;
}

package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;
import com.google.common.base.Predicate;
import com.jivesoftware.os.tasmo.id.BaseView;

/**
 * Determines whether a view change should be processed.
 */
public interface ViewMatcher<V extends BaseView<?>> extends Predicate<ViewChange<V>> {

}

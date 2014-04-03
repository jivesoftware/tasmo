package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.google.common.base.Predicate;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.view.notification.api.ViewChange;

/**
 * Determines whether a view change should be processed.
 */
public interface ViewMatcher<V extends BaseView<?>> extends Predicate<ViewChange<V>> {

}

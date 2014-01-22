package com.jivesoftware.os.tasmo.id;

/**
 * Base implementation for all views.
 *
 * @param <E> event type that the view is rooted on
 */
public interface BaseView<E extends BaseEvent> {

    Ref<E> viewBase();
}

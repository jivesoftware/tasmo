package com.jivesoftware.os.tasmo.view.reader.api;

import com.jivesoftware.os.tasmo.id.BaseEvent;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tagging annotation used to flag fields in a view which are defined as backward
 * relations from the referenced event to the current view's event.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BackRef {

    /** The event class which should be consulted for the back-reference. */
    Class<? extends BaseEvent> from();

    /** The name of the field in the event referenced by {@link #from} */
    String via();

    /** Type of the back-ref (all, latest etc.) */
    BackRefType type();

}

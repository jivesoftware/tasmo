package com.jivesoftware.os.tasmo.id;

/**
 * Interface used to define fields common to all events.
 */
public interface BaseEvent {

    /**
     * Event/object ID's numeric component.  The event name component is inherited from the
     * event class' simple name.
     *
     * @return Object ID numeric value
     */
    // NOTE: Must match ReservedFields.INSTANCE_ID
    Id instanceId();

    /**
     * Flag indicating that the event object should be deleted form the system.  This is
     * a soft delete which prevents the data from being returned to down-stream readers.
     *
     * @return {@code true} if the record should reflect a deleted state, {@code false} otherwise
     */
    // NOTE: Must match ReservedFields.DELETED
    boolean deleted();

}

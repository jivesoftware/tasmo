package com.jivesoftware.os.tasmo.model.process;

/**
 * Represents a field value modified by an event which is not examined by the tasmo. It is round tripped unchanged between the incoming event and the
 * resultant view.
 */
public interface OpaqueFieldValue {
    boolean isNull();
}

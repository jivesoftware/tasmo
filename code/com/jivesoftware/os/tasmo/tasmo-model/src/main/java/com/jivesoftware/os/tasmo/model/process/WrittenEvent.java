package com.jivesoftware.os.tasmo.model.process;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;

/**
 * Represents one write against one logical object. This is the unit of processing within Tasmo. Events have header fields and a payload. Payloads contain both
 * the identity of the logical object being written to as well as the fields being updated. Field names in a payload are strings. Field values are either single
 * references to other objects, arrays of references to other objects, or literal field values which are opaque to Tasmo.
 *
 */
public interface WrittenEvent {

    long getEventId();

    Id getActorId();

    TenantId getTenantId();

    Id getCentricId();

    WrittenInstance getWrittenInstance();

    boolean isBookKeepingEnabled();
}

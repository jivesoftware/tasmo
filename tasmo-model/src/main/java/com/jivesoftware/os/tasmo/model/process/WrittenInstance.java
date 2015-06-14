package com.jivesoftware.os.tasmo.model.process;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;

/**
 * Represents the logical object instance being modified by an incoming event. This contains the actual data object fields being modified as well as the
 * identity of the object within the system.
 */
public interface WrittenInstance {

    ObjectId getInstanceId();

    Iterable<String> getFieldNames();

    OpaqueFieldValue getFieldValue(String fieldName);

    ObjectId getReferenceFieldValue(String fieldName);

    ObjectId[] getMultiReferenceFieldValue(String fieldName);

    Id getIdFieldValue(String fieldName);

    boolean hasField(String fieldName);

    void removeField(String fielName);

    boolean isDeletion();
}

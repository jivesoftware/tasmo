package com.jivesoftware.os.tasmo.model.process;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Collection;

/**
 * Represents the logical object instance being modified by an incoming event. This contains the actual data object fields being modified as well as the
 * identity of the object within the system.
 */
public interface WrittenInstance {

    ObjectId getInstanceId();

    Collection<String> getFieldNames();

    OpaqueFieldValue getFieldValue(String fieldName);

    ObjectId getReferenceFieldValue(String fieldName);

    ObjectId[] getMultiReferenceFieldValue(String fieldName);

    Id getIdFieldValue(String fieldName);

    boolean hasField(String fieldName);

    boolean isDeletion();
}

package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.ObjectId;

/**
 *
 *
 */
public class Event {
    private final ObjectNode event;
    private final ObjectId objectId;

    Event(ObjectNode event, ObjectId objectId) {
        this.event = event;
        this.objectId = objectId;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public ObjectNode toJson() {
        return event;
    }

    @Override
    public String toString() {
        return event.toString();
    }
}

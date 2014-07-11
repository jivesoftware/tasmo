/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.event.api.write;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class EventWriterResponse {

    private final List<Long> eventIds;
    private final List<ObjectId> objectIds;

    public EventWriterResponse(List<Long> eventIds, List<ObjectId> objectIds) {
        if (eventIds.size() != objectIds.size()) {
            throw new IllegalArgumentException("It is expected that writable, eventIds, and objectIds are the same size and index aligned.");
        }
        this.eventIds = eventIds;
        this.objectIds = objectIds;
    }

    public List<Long> getEventIds() {
        return eventIds;
    }

    public List<ObjectId> getObjectIds() {
        return objectIds;
    }


    @Override
    public String toString() {
        return "EventWriterResponse{"
            + "eventIds=" + eventIds
            + ", objectIds=" + objectIds
            + '}';
    }
}

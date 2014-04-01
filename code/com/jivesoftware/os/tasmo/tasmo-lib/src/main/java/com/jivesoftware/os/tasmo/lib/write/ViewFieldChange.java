/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.write;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * This is the write side of com.jivesoftware.soa.modules.view.writer.ViewWriteFieldChange. If you changes this you will likely need to change
 * ViewWriteFieldChange.
 */
public class ViewFieldChange {

    static public enum ViewFieldChangeType {

        add, remove
    }
    private final long eventId;
    private final Id actorId;
    private final ViewFieldChangeType type;
    private final ObjectId viewObjectId;
    private final String modelPathId;
    private final PathId[] modelPathInstanceIds;
    private final List<ReferenceWithTimestamp> modelPathVersions;
    private final String value;
    private final long timestamp;

    @JsonCreator
    public ViewFieldChange(
            @JsonProperty("eventId") long eventId,
            @JsonProperty("actorId") Id actorId,
            @JsonProperty("type") ViewFieldChangeType type,
            @JsonProperty("viewObjectId") ObjectId viewObjectId,
            @JsonProperty("modelPathId") String modelPathId,
            @JsonProperty("modelPathInstanceIds") PathId[] modelPathInstanceIds,
            @JsonProperty("modelPathVersions") List<ReferenceWithTimestamp> modelPathVersions,
            @JsonProperty("value") String value,
            @JsonProperty("timestamp") long timestamp) {
        this.eventId = eventId;
        this.actorId = actorId;
        this.type = type;
        this.viewObjectId = viewObjectId;
        this.modelPathId = modelPathId;
        this.modelPathInstanceIds = modelPathInstanceIds;
        this.modelPathVersions = modelPathVersions;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getEventId() {
        return eventId;
    }

    public Id getActorId() {
        return actorId;
    }

    public ViewFieldChangeType getType() {
        return type;
    }

    public ObjectId getViewObjectId() {
        return viewObjectId;
    }

    public String getModelPathId() {
        return modelPathId;
    }

    public PathId[] getModelPathInstanceIds() {
        return modelPathInstanceIds;
    }

    public List<ReferenceWithTimestamp> getModelPathVersions() {
        return modelPathVersions;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ViewFieldChange{"
                + "eventId=" + eventId
                + ", actorId=" + actorId
                + ", type=" + type
                + ", viewObjectId=" + viewObjectId
                + ", modelPathId=" + modelPathId
                + ", modelPathInstanceIds=" + Arrays.deepToString(modelPathInstanceIds)
                + ", value=" + value
                + ", timestamp=" + timestamp + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + (int) (this.eventId ^ (this.eventId >>> 32));
        hash = 97 * hash + Objects.hashCode(this.actorId);
        hash = 97 * hash + (this.type != null ? this.type.hashCode() : 0);
        hash = 97 * hash + Objects.hashCode(this.viewObjectId);
        hash = 97 * hash + Objects.hashCode(this.modelPathId);
        hash = 97 * hash + Arrays.deepHashCode(this.modelPathInstanceIds);
        hash = 97 * hash + Objects.hashCode(this.value);
        hash = 97 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ViewFieldChange other = (ViewFieldChange) obj;
        if (this.eventId != other.eventId) {
            return false;
        }
        if (!Objects.equals(this.actorId, other.actorId)) {
            return false;
        }
        if (this.type != other.type) {
            return false;
        }
        if (!Objects.equals(this.viewObjectId, other.viewObjectId)) {
            return false;
        }
        if (!Objects.equals(this.modelPathId, other.modelPathId)) {
            return false;
        }
        if (!Arrays.deepEquals(this.modelPathInstanceIds, other.modelPathInstanceIds)) {
            return false;
        }
        if (!Objects.equals(this.value, other.value)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }
}

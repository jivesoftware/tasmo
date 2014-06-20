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
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
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
    private final long modelPathIdHashcode;
    private final PathId[] modelPathInstanceIds;
    private final List<ReferenceWithTimestamp> modelPathVersions;
    private final long[] modelPathTimestamps;
    private final byte[] value;
    private final long timestamp;

    @JsonCreator
    public ViewFieldChange(long eventId,
            Id actorId,
            ViewFieldChangeType type,
            ObjectId viewObjectId,
            long modelPathIdHashcode,
            PathId[] modelPathInstanceIds,
            List<ReferenceWithTimestamp> modelPathVersions,
            long[] modelPathTimestamps,
            byte[] value,
            long timestamp) {
        this.eventId = eventId;
        this.actorId = actorId;
        this.type = type;
        this.viewObjectId = viewObjectId;
        this.modelPathIdHashcode = modelPathIdHashcode;
        this.modelPathInstanceIds = modelPathInstanceIds;
        this.modelPathVersions = modelPathVersions;
        this.modelPathTimestamps = modelPathTimestamps;
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

    public long getModelPathIdHashcode() {
        return modelPathIdHashcode;
    }

    public PathId[] getModelPathInstanceIds() {
        return modelPathInstanceIds;
    }

    public List<ReferenceWithTimestamp> getModelPathVersions() {
        return modelPathVersions;
    }

    public long[] getModelPathTimestamps() {
        return modelPathTimestamps;
    }

    public byte[] getValue() {
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
                + ", modelPathIdHashcode=" + modelPathIdHashcode
                + ", modelPathInstanceIds=" + Arrays.deepToString(modelPathInstanceIds)
                + ", modelPathTimestamps=" + Arrays.toString(modelPathTimestamps)
                + ", value=" + new String(value)
                + ", timestamp=" + timestamp + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (int) (this.eventId ^ (this.eventId >>> 32));
        hash = 59 * hash + Objects.hashCode(this.actorId);
        hash = 59 * hash + Objects.hashCode(this.type);
        hash = 59 * hash + Objects.hashCode(this.viewObjectId);
        hash = 59 * hash + (int) (this.modelPathIdHashcode ^ (this.modelPathIdHashcode >>> 32));
        hash = 59 * hash + Arrays.deepHashCode(this.modelPathInstanceIds);
        hash = 59 * hash + Objects.hashCode(this.modelPathVersions);
        hash = 59 * hash + Arrays.hashCode(this.modelPathTimestamps);
        hash = 59 * hash + Arrays.hashCode(this.value);
        hash = 59 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
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
        if (this.modelPathIdHashcode != other.modelPathIdHashcode) {
            return false;
        }
        if (!Arrays.deepEquals(this.modelPathInstanceIds, other.modelPathInstanceIds)) {
            return false;
        }
        if (!Objects.equals(this.modelPathVersions, other.modelPathVersions)) {
            return false;
        }
        if (!Arrays.equals(this.modelPathTimestamps, other.modelPathTimestamps)) {
            return false;
        }
        if (!Arrays.equals(this.value, other.value)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

}

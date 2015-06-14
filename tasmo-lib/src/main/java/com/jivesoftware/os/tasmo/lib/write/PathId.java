package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.Objects;

/**
 *
 * @author jonathan
 */
public class PathId {

    private final ObjectId objectId;
    private final long timestamp;

    public PathId(ObjectId objectId, long timestamp) {
        this.objectId = objectId;
        this.timestamp = timestamp;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return objectId + " @ " + timestamp;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.objectId);
        hash = 19 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
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
        final PathId other = (PathId) obj;
        if (!Objects.equals(this.objectId, other.objectId)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

}

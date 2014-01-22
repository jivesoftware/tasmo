package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.Objects;

public class Reference {

    private final ObjectId objectId;
    private final long timestamp;

    public Reference(ObjectId objectId, long timestamp) {
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
        return "Reference{" + "objectId=" + objectId + ", timestamp=" + timestamp + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 73 * hash + Objects.hashCode(this.objectId);
        hash = 73 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
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
        final Reference other = (Reference) obj;
        if (!Objects.equals(this.objectId, other.objectId)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }
}

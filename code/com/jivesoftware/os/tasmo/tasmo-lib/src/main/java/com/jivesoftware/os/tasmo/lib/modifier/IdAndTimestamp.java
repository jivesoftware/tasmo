package com.jivesoftware.os.tasmo.lib.modifier;

import com.jivesoftware.os.jive.utils.id.Id;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class IdAndTimestamp {

    private final Id id;
    private final long timestamp;

    public IdAndTimestamp(Id id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public Id getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "IdAndTimestamp{" + "id=" + id + ", timestamp=" + timestamp + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.id);
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
        final IdAndTimestamp other = (IdAndTimestamp) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

}

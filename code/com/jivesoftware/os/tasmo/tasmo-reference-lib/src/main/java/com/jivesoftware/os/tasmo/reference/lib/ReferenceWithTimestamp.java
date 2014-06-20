package com.jivesoftware.os.tasmo.reference.lib;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.Objects;

public class ReferenceWithTimestamp {

    private final ObjectId objectId;
    private final String fieldName;
    private final long timestamp;
    private final String pathToCreator;

    @JsonCreator
    public ReferenceWithTimestamp(@JsonProperty("objectId") ObjectId objectId,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("timestamp") long timestamp) {
        this.objectId = objectId;
        this.fieldName = fieldName;
        this.timestamp = timestamp;
        this.pathToCreator = ""; //Debug.caller(10);
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Reference{" + "objectId=" + objectId + ", fieldName=" + fieldName + ", timestamp=" + timestamp + '}' + pathToCreator;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.objectId);
        hash = 19 * hash + Objects.hashCode(this.fieldName);
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
        final ReferenceWithTimestamp other = (ReferenceWithTimestamp) obj;
        if (!Objects.equals(this.objectId, other.objectId)) {
            return false;
        }
        if (!Objects.equals(this.fieldName, other.fieldName)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

}

package com.jivesoftware.os.tasmo.reference.lib;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import java.util.Objects;

public class Reference {

    private final ObjectId objectId;
    private final String fieldName;

    @JsonCreator
    public Reference(@JsonProperty("objectId") ObjectId objectId,
            @JsonProperty("fieldName") String fieldName) {
        this.objectId = objectId;
        this.fieldName = fieldName;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "Reference{" + "objectId=" + objectId + ", fieldName=" + fieldName + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.objectId);
        hash = 19 * hash + Objects.hashCode(this.fieldName);
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
        if (!Objects.equals(this.fieldName, other.fieldName)) {
            return false;
        }
        return true;
    }

}

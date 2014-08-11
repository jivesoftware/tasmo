package com.jivesoftware.os.tasmo.reference.lib;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.Objects;

public class ReferenceWithTimestamp {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final ObjectId objectId;
    private final String fieldName;
    private final long timestamp;

    @JsonCreator
    public ReferenceWithTimestamp(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId objectId,
            String fieldName,
            long timestamp) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.objectId = objectId;
        this.fieldName = fieldName;
        this.timestamp = timestamp;
    }

    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return tenantIdAndCentricId;
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
        return "ReferenceWithTimestamp{"
                + "tenantIdAndCentricId=" + tenantIdAndCentricId
                + ", objectId=" + objectId
                + ", fieldName=" + fieldName
                + ", timestamp=" + timestamp
                + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.tenantIdAndCentricId);
        hash = 37 * hash + Objects.hashCode(this.objectId);
        hash = 37 * hash + Objects.hashCode(this.fieldName);
        hash = 37 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
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
        if (!Objects.equals(this.tenantIdAndCentricId, other.tenantIdAndCentricId)) {
            return false;
        }
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

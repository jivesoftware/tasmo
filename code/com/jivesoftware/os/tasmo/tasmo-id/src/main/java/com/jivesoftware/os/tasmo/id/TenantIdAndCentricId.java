package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;

public class TenantIdAndCentricId implements Comparable<TenantIdAndCentricId> {

    private final TenantId tenantId;
    private final Id centricId;

    @JsonCreator
    public TenantIdAndCentricId(
            @JsonProperty("tenantId") TenantId tenantId,
            @JsonProperty("centricId") Id centricId) {
        this.tenantId = Preconditions.checkNotNull(tenantId, "tenantId cannot be null");
        this.centricId = Preconditions.checkNotNull(centricId, "userId cannot be null");
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    public Id getCentricId() {
        return centricId;
    }

    @Override
    public String toString() {
        return "TenantIdAndCentricId{" + "tenantId=" + tenantId + ", centricId=" + centricId + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + Objects.hashCode(this.tenantId);
        hash = 79 * hash + Objects.hashCode(this.centricId);
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
        final TenantIdAndCentricId other = (TenantIdAndCentricId) obj;
        if (!Objects.equals(this.tenantId, other.tenantId)) {
            return false;
        }
        if (!Objects.equals(this.centricId, other.centricId)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(TenantIdAndCentricId o) {
        int i = this.tenantId.compareTo(o.tenantId);
        if (i == 0) {
            i = this.centricId.compareTo(o.centricId);
        }
        return i;
    }
}

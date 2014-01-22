package com.jivesoftware.os.tasmo.view.reader.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.Objects;

public final class ViewDescriptor {

    private final TenantId tenantId;
    private final Id actorId;
    private final ObjectId viewId;
    private final Id userId;

    public ViewDescriptor(
            @JsonProperty("tenantId") TenantId tenantId,
            @JsonProperty("actorId") Id actorId,
            @JsonProperty("viewId") ObjectId viewId,
            @JsonProperty("userId") Id userId) {
        this.tenantId = Preconditions.checkNotNull(tenantId, "tenantId cannot be null");
        this.actorId = Preconditions.checkNotNull(actorId, "actorId cannot be null");
        this.viewId = Preconditions.checkNotNull(viewId, "viewId cannot be null");
        this.userId = Preconditions.checkNotNull(userId, "userId cannot be null");
    }

    @JsonIgnore
    public ViewDescriptor(
            TenantId tenantId,
            Id actorId,
            ObjectId viewId) {
        this(tenantId, actorId, viewId, Id.NULL);
    }

    @JsonIgnore
    public ViewDescriptor(
        TenantId tenantId,
        Id actorId,
        ViewId<?> viewId) {
        this(tenantId, actorId, viewId.asObjectId(), Id.NULL);
    }

    @JsonIgnore
    public ViewDescriptor(
        TenantId tenantId,
        Id actorId,
        ViewId<?> viewId,
        Id userId) {
        this(tenantId, actorId, viewId.asObjectId(), userId);
    }

    @JsonIgnore
    public ViewDescriptor(
            TenantIdAndCentricId tenantIdAndCentricId,
            Id actorId,
            ObjectId viewId) {
        this(tenantIdAndCentricId.getTenantId(), actorId, viewId, tenantIdAndCentricId.getCentricId());
    }

    @JsonIgnore
    public ViewDescriptor(
        TenantIdAndCentricId tenantIdAndCentricId,
        Id actorId,
        ViewId<?> viewId) {
        this(tenantIdAndCentricId.getTenantId(), actorId, viewId, tenantIdAndCentricId.getCentricId());
    }

    @JsonIgnore
    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return new TenantIdAndCentricId(tenantId, userId);
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    public Id getUserId() {
        return userId;
    }

    public Id getActorId() {
        return actorId;
    }

    public ObjectId getViewId() {
        return viewId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, actorId, viewId, userId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ViewDescriptor other = (ViewDescriptor) obj;
        return Objects.equals(this.tenantId, other.tenantId)
                && Objects.equals(this.actorId, other.actorId)
                && Objects.equals(this.viewId, other.viewId)
                && Objects.equals(this.userId, other.userId);
    }

    @Override
    public String toString() {
        return "ViewDescriptor{" + "tenantId=" + tenantId + ", actorId=" + actorId + ", viewId=" + viewId + ", userId=" + userId + '}';
    }
}

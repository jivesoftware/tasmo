package com.jivesoftware.os.tasmo.view.notification.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.Objects;

public class ViewNotification {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final long eventId;
    private final Id actorId;
    private final Id userId;
    private final ObjectId viewId;
    private final boolean centric;

    public ViewNotification(
            @JsonProperty("tenantIdAndCentricId") TenantIdAndCentricId tenantIdAndCentricId,
            @JsonProperty("eventId") long eventId,
            @JsonProperty("actorId") Id actorId,
            @JsonProperty("userId") Id userId,
            @JsonProperty("viewId") ObjectId viewId,
            @JsonProperty("centric") boolean centric
    ) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.eventId = eventId;
        this.actorId = actorId;
        this.userId = userId;
        this.viewId = viewId;
        this.centric = centric;
    }

    public TenantIdAndCentricId getTenantIdAndCentricId() {
        return tenantIdAndCentricId;
    }

    public long getEventId() {
        return eventId;
    }

    public Id getActorId() {
        return actorId;
    }

    public Id getUserId() {
        return userId;
    }

    public ObjectId getViewId() {
        return viewId;
    }

    public boolean getCentric() {
        return centric;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ViewNotification other = (ViewNotification) obj;
        if (this.eventId != other.eventId) {
            return false;
        }
        if (!Objects.equals(this.actorId, other.actorId)) {
            return false;
        }
        if (!Objects.equals(this.userId, other.userId)) {
            return false;
        }
        if (!Objects.equals(this.viewId, other.viewId)) {
            return false;
        }
        if (this.centric != other.centric) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + (int) (this.eventId ^ (this.eventId >>> 32));
        hash = 17 * hash + Objects.hashCode(this.actorId);
        hash = 17 * hash + Objects.hashCode(this.userId);
        hash = 17 * hash + Objects.hashCode(this.viewId);
        hash = 17 * hash + (this.centric ? 1 : 0);
        return hash;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ViewNotification{");
        sb.append("tenantIdAndCentricId=").append(tenantIdAndCentricId);
        sb.append(", eventId=").append(eventId);
        sb.append(", actorId=").append(actorId);
        sb.append(", userId=").append(userId);
        sb.append(", viewId=").append(viewId);
        sb.append(", centric=").append(centric);
        sb.append('}');
        return sb.toString();
    }
}

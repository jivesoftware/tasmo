package com.jivesoftware.os.tasmo.view.notification.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;

public class ViewNotification {

    private final TenantIdAndCentricId tenantIdAndCentricId;
    private final long eventId;
    private final Id actorId;
    private final ObjectId viewId;

    public ViewNotification(
        @JsonProperty("tenantIdAndCentricId") TenantIdAndCentricId tenantIdAndCentricId,
        @JsonProperty("eventId") long eventId,
        @JsonProperty("actorId") Id actorId,
        @JsonProperty("viewId") ObjectId viewId
        ) {
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.eventId = eventId;
        this.actorId = actorId;
        this.viewId = viewId;
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

    public ObjectId getViewId() {
        return viewId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ViewNotification that = (ViewNotification) o;

        if (eventId != that.eventId) {
            return false;
        }
        if (!actorId.equals(that.actorId)) {
            return false;
        }
        if (!tenantIdAndCentricId.equals(that.tenantIdAndCentricId)) {
            return false;
        }
        if (!viewId.equals(that.viewId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantIdAndCentricId.hashCode();
        result = 31 * result + (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + actorId.hashCode();
        result = 31 * result + viewId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ViewNotification{");
        sb.append("tenantIdAndCentricId=").append(tenantIdAndCentricId);
        sb.append(", eventId=").append(eventId);
        sb.append(", actorId=").append(actorId);
        sb.append(", viewId=").append(viewId);
        sb.append('}');
        return sb.toString();
    }
}

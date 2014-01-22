/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.bookkeeping;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.Objects;

public class BookkeepingEvent {

    private final TenantId tenantId;
    private final Id actorId;
    private final long eventId;
    private final boolean successful;

    public BookkeepingEvent(
            @JsonProperty("tenantId") TenantId tenantId,
            @JsonProperty("actorId") Id actorId,
            @JsonProperty("eventId") long eventId,
            @JsonProperty("status") boolean successful) {
        this.tenantId = tenantId;
        this.actorId = actorId;
        this.eventId = eventId;
        this.successful = successful;
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    public Id getActorId() {
        return actorId;
    }

    public long getEventId() {
        return eventId;
    }

    public boolean isSuccessful() {
        return successful;
    }

    @Override
    public String toString() {
        return "BookkeepingEvent{" + "tenantId=" + tenantId + ", actorId=" + actorId + ", eventId=" + eventId
                + ", successful=" + successful + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.tenantId);
        hash = 59 * hash + Objects.hashCode(this.actorId);
        hash = 59 * hash + (int) (this.eventId ^ (this.eventId >>> 32));
        hash = 59 * hash + (this.successful ? 1 : 0);
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
        final BookkeepingEvent other = (BookkeepingEvent) obj;
        if (!Objects.equals(this.tenantId, other.tenantId)) {
            return false;
        }
        if (!Objects.equals(this.actorId, other.actorId)) {
            return false;
        }
        if (this.eventId != other.eventId) {
            return false;
        }
        if (this.successful != other.successful) {
            return false;
        }
        return true;
    }
}

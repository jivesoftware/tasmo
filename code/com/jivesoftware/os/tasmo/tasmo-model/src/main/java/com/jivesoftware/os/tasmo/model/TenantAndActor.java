package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class TenantAndActor {
    public final TenantId tenantId;
    public final Id actorId;

    public TenantAndActor(TenantId tenantId, Id actorId) {
        this.tenantId = tenantId;
        this.actorId = actorId;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.tenantId);
        hash = 79 * hash + Objects.hashCode(this.actorId);
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
        final TenantAndActor other = (TenantAndActor) obj;
        if (!Objects.equals(this.tenantId, other.tenantId)) {
            return false;
        }
        if (!Objects.equals(this.actorId, other.actorId)) {
            return false;
        }
        return true;
    }

}

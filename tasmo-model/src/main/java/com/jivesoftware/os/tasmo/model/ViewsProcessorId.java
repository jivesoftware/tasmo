/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.jive.utils.id.TenantId;
import java.util.Objects;

public class ViewsProcessorId {

    private final TenantId tenantId;
    private final String processorId;

    public ViewsProcessorId(TenantId tenantId, String processorId) {
        this.tenantId = tenantId;
        this.processorId = processorId;
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    public String getProcessorId() {
        return processorId;
    }

    @Override
    public String toString() {
        return "ViewsProcessorId{"
            + "tenantId=" + tenantId
            + ", processorId=" + processorId
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + Objects.hashCode(this.tenantId);
        hash = 71 * hash + Objects.hashCode(this.processorId);
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
        final ViewsProcessorId other = (ViewsProcessorId) obj;
        if (!Objects.equals(this.tenantId, other.tenantId)) {
            return false;
        }
        if (!Objects.equals(this.processorId, other.processorId)) {
            return false;
        }
        return true;
    }
}

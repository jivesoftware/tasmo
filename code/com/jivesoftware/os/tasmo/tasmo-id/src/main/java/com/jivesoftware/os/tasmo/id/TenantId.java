/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

/**
 * Represents a tenant in the system.
 */
public class TenantId implements Comparable<TenantId> {
    private final String tenantId;

    @JsonCreator
    public TenantId(String tenantId) {
        Preconditions.checkNotNull(tenantId);
        tenantId = tenantId.trim();
        Preconditions.checkArgument(tenantId.length() > 0);
        this.tenantId = tenantId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TenantId tenantId = (TenantId) o;
        return this.tenantId.equals(tenantId.tenantId);
    }

    @Override
    public int hashCode() {
        return tenantId != null ? tenantId.hashCode() : 0;
    }

    @JsonValue
    public String toStringForm() {
        return tenantId;
    }

    @Override
    public String toString() {
        return tenantId;
    }

    @Override
    public int compareTo(TenantId o) {
        return this.tenantId.compareTo(o.tenantId);
    }
}

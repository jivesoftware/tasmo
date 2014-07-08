/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.TenantId;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class Views {

    private final TenantId tenantId;
    private final ChainedVersion version;
    private final List<ViewBinding> viewBindings;

    @JsonCreator
    public Views(
        @JsonProperty ("tenantId") TenantId tenantId,
        @JsonProperty ("version") ChainedVersion version,
        @JsonProperty ("viewBindings") List<ViewBinding> viewBindings) {
        this.tenantId = tenantId;
        this.version = version;
        this.viewBindings = viewBindings;
    }

    public TenantId getTenantId() {
        return tenantId;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public List<ViewBinding> getViewBindings() {
        return Collections.unmodifiableList(viewBindings);
    }

    @Override
    public String toString() {
        return "Views{"
            + "tenantId=" + tenantId
            + ", version=" + version
            + ", viewBindings=" + viewBindings
            + '}';
    }
}

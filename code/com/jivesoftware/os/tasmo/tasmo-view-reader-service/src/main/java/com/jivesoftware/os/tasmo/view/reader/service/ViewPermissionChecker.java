/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.Set;

public interface ViewPermissionChecker {

    /**
     *  Returns a set of ids that are equal to or a subset of the
     * input set of ids which the actor has permission to view.
     *
     *
     * @param tenantId
     * @param actorId
     * @param permissionCheckTheseIds
     * @return
     */
    public ViewPermissionCheckResult check(TenantId tenantId, Id actorId, Set<Id> permissionCheckTheseIds);
}

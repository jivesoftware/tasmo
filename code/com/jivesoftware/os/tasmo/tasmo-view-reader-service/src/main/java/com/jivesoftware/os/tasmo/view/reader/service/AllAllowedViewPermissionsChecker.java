package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class AllAllowedViewPermissionsChecker implements ViewPermissionChecker {

    @Override
    public ViewPermissionCheckResult check(TenantId tenantId, Id actorId, final Set<Id> permissionCheckTheseIds) {
        //System.out.println("NO-OP permisions check for (" + permissionCheckTheseIds.size() + ") ids.");
        return new ViewPermissionCheckResult() {
            @Override
            public Set<Id> allowed() {
                return permissionCheckTheseIds;
            }

            @Override
            public Set<Id> denied() {
                return new HashSet<>();
            }

            @Override
            public Set<Id> unknown() {
                return new HashSet<>();
            }
        };
    }
}

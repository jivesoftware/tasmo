/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.TenantId;


public interface ViewsProvider {

    /**
     *
     * @param tenantId
     * @return the current version or ChainedVersion.NULL;
     */
    ChainedVersion getCurrentViewsVersion(TenantId tenantId);

    /**
     *
     * @param viewsProcessorId
     * @return the current view or null
     */
    Views getViews(ViewsProcessorId viewsProcessorId);
}

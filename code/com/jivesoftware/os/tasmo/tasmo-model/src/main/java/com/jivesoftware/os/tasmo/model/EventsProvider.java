/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.model;

import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.List;

public interface EventsProvider {
    /**
     *
     * @param tenantId
     * @return the current version or ChainedVersion.NULL;
     */
    ChainedVersion getCurrentEventsVersion(TenantId tenantId);

    /**
     *
     * @param eventsProcessorId
     * @return the current view or null
     */
    List<EventDefinition> getEvents(EventsProcessorId eventsProcessorId);
}


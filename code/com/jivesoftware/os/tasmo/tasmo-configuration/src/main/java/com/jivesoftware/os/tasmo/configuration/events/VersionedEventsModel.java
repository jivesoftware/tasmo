/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.configuration.events;

import com.jivesoftware.os.tasmo.configuration.EventsModel;
import com.jivesoftware.os.tasmo.id.ChainedVersion;

/**
 *
 * @author jonathan.colt
 */
public class VersionedEventsModel {
    private final ChainedVersion version;
    private final EventsModel eventsModel;

    public VersionedEventsModel(ChainedVersion version, EventsModel eventsModel) {
        this.version = version;
        this.eventsModel = eventsModel;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public EventsModel getEventsModel() {
        return eventsModel;
    }

    @Override
    public String toString() {
        return "VersionedEventsModel{"
            + "version=" + version
            + ", eventsModel="
            + eventsModel
            + '}';
    }

}

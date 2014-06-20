package com.jivesoftware.os.tasmo.configuration.events;

import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.tasmo.configuration.EventsModel;

/**
 *
 * @author jonathan.colt
 */
public class VersionedEventsModel {
    private final ChainedVersion version;
    private final EventsModel eventsModel;

    VersionedEventsModel(ChainedVersion version, EventsModel eventsModel) {
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

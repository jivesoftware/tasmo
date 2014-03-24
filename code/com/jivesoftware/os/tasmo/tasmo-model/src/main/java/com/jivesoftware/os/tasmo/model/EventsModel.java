package com.jivesoftware.os.tasmo.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EventsModel {

    private final Map<String, EventDefinition> pantheon = new HashMap<>();

    public boolean isEmpty() {
        return pantheon.isEmpty();
    }

    public EventDefinition getEvent(String className) {
        return pantheon.get(className);
    }

    public Set<String> getAllEventClassNames() {
        return pantheon.keySet();
    }

    /*
    public void addEvent(ObjectNode eventNode) {
        EventDefinition eventConfiguration = EventDefinition.builder(eventNode, true).build();
        pantheon.put(eventConfiguration.getEventClass(), eventConfiguration);
    }
    */

    public void addEvent(EventDefinition eventConfiguration) {
        pantheon.put(eventConfiguration.getEventClass(), eventConfiguration);
    }
}

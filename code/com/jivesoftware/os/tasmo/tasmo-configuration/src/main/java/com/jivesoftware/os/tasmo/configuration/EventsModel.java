package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EventsModel {

    private final Map<String, EventModel> pantheon = new HashMap<>();

    public boolean isEmpty() {
        return pantheon.isEmpty();
    }

    public EventModel getEvent(String className) {
        return pantheon.get(className);
    }

    public Set<String> getAllEventClassNames() {
        return pantheon.keySet();
    }

    public void addEvent(ObjectNode eventNode) {
        EventModel eventConfiguration = EventModel.builder(eventNode, true).build();
        addEvent(eventConfiguration);
    }

    public void addEvent(EventModel eventConfiguration) {
        pantheon.put(eventConfiguration.getEventClass(), eventConfiguration);
    }
}

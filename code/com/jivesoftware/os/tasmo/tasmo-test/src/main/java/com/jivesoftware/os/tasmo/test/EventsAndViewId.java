package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.jive.utils.id.Id;
import java.util.List;

public class EventsAndViewId {
    private final Id viewId;
    private final List<Event> events;
    private final IdTreeNode idTree;

    public EventsAndViewId(Id viewId, List<Event> events, IdTreeNode idTree) {
        this.viewId = viewId;
        this.events = events;
        this.idTree = idTree;
    }

    public Id getViewId() {
        return viewId;
    }

    public List<Event> getEvents() {
        return events;
    }

    public IdTreeNode getIdTree() {
        return idTree;
    }

}

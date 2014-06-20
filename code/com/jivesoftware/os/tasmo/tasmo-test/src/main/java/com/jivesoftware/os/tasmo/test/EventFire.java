/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.tasmo.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventFire {
    private final List<Event> events;
    private final ObjectId viewId;
    private final ModelPathStep leafNode;
    private final IdTreeNode idTree;

    public EventFire(ObjectId viewId, List<Event> events, ModelPathStep leafNode, IdTreeNode idTree) {
        this.events = new ArrayList<>(events);
        this.viewId = viewId;
        this.leafNode = leafNode;
        this.idTree = idTree;
    }

    public List<Event> getFiredEvents() {
        return events;
    }

    public IdTreeNode getIdTree() {
        return idTree;
    }

    public List<Event> createDeletesAtDepth(TenantId tenant, Id actor, int depth, int breadth) {
        List<Event> deleteEvents = new ArrayList<>();
        Set<Id> idsAtDepth = new HashSet<>();
        idTree.accumulateAtDepth(idsAtDepth, 0, depth);
        for (Event evt : events) {
            ObjectId objectId = evt.getObjectId();
            if (idsAtDepth.contains(objectId.getId())) {
                Event deleteEvent = EventBuilder.update(objectId, tenant, actor).set(ReservedFields.DELETED, true).build();
                deleteEvents.add(deleteEvent);
            }
        }
        if (breadth > 0) {
            return deleteEvents.subList(0, Math.min(breadth, deleteEvents.size()));
        } else {
            return deleteEvents;
        }
    }

    public Map<String, String> getLeafNodeFields() {
        Map<String, String> leafNodeFields = new HashMap<>();
        for (Event evt : events) {
            ObjectNode json = evt.toJson();
            for (String originClassname : leafNode.getOriginClassNames()) {
                if (json.has(originClassname)) {
                    json = (ObjectNode) json.get(originClassname);
                    for (String fieldName : leafNode.getFieldNames()) {
                        JsonNode fieldVal = json.get(fieldName);
                        if (fieldVal != null) {
                            leafNodeFields.put(fieldName, json.get(fieldName).asText());
                        }
                    }
                }
            }
        }
        if (leafNodeFields.size() < leafNode.getFieldNames().size()) {
            throw new IllegalStateException("Event fire doesn't contain an event which populates the leaf node: " + leafNode);
        }
        return leafNodeFields;
    }

    public ObjectId getViewId() {
        return viewId;
    }

}

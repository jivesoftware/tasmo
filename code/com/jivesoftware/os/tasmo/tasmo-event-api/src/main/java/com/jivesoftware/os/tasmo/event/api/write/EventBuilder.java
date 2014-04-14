/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.tasmo.event.api.EventVerb;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.IdProvider;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.Collection;

/**
 *
 */
public class EventBuilder {

    private static final JsonEventConventions JSON_EVENT_CONVENTIONS = new JsonEventConventions();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final ObjectNode eventNode = OBJECT_MAPPER.createObjectNode();
    private final ObjectId objectId;
    private final ObjectNode instanceNode;

    public static EventBuilder create(IdProvider idProvider, String className, TenantId tenantId, Id userAndActorId) {
        return new EventBuilder(new ObjectId(className, idProvider.nextId()), tenantId, userAndActorId, userAndActorId, EventVerb.CREATED);
    }

    public static EventBuilder create(IdProvider idProvider, String className, TenantId tenantId, Id actorId, Id userId) {
        return new EventBuilder(new ObjectId(className, idProvider.nextId()), tenantId, actorId, userId, EventVerb.CREATED);
    }

    public static EventBuilder update(ObjectId objectId, TenantId tenantId, Id userAndActorId) {
        return new EventBuilder(objectId, tenantId, userAndActorId, userAndActorId, EventVerb.MODIFIED);
    }

    public static EventBuilder update(ObjectId objectId, TenantId tenantId, Id actorId, Id userId) {
        return new EventBuilder(objectId, tenantId, actorId, userId, EventVerb.MODIFIED);
    }

    public static EventBuilder migrate(ObjectId objectId, long migrationEventId, TenantId tenantId, Id userAndActorId) {
        return new EventBuilder(objectId, migrationEventId, tenantId, userAndActorId, userAndActorId, EventVerb.MIGRATED);
    }

    public static EventBuilder migrate(ObjectId objectId, long migrationEventId, TenantId tenantId, Id actorId, Id userId) {
        return new EventBuilder(objectId, migrationEventId, tenantId, actorId, userId, EventVerb.MIGRATED);
    }

    private EventBuilder(ObjectId objectId, long migrationEventId, TenantId tenantId, Id actorId, Id userId, EventVerb eventVerb) {
        this(objectId, tenantId, actorId, userId, eventVerb);
        JSON_EVENT_CONVENTIONS.setEventId(eventNode, migrationEventId);
    }

    private EventBuilder(ObjectId objectId, TenantId tenantId, Id actorId, Id userId, EventVerb eventVerb) {
        Preconditions.checkNotNull(objectId);
        Preconditions.checkNotNull(tenantId);
        Preconditions.checkNotNull(actorId);
        Preconditions.checkNotNull(userId);
        Preconditions.checkNotNull(eventVerb);

        instanceNode = JSON_EVENT_CONVENTIONS.setInstanceClassName(eventNode, objectId.getClassName());
        JSON_EVENT_CONVENTIONS.setInstanceId(eventNode, objectId.getId(), objectId.getClassName());
        JSON_EVENT_CONVENTIONS.setTenantId(eventNode, tenantId);
        JSON_EVENT_CONVENTIONS.setActorId(eventNode, actorId);
        JSON_EVENT_CONVENTIONS.setUserId(eventNode, userId);
        JSON_EVENT_CONVENTIONS.setActivityVerb(eventNode, eventVerb);
        this.objectId = objectId;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public EventBuilder delete(String key) {
        instanceNode.putNull(key);
        return this;
    }

    public EventBuilder clear(String key) {
        instanceNode.put(key, (String) null);
        return this;
    }

    public <V> EventBuilder set(String key, V value) {
        if (value == null) {
            throw new IllegalStateException("null would result in the stored value for this field:" + key + " being removed. Use clear(String key) instead.");
        }

        if (value instanceof ObjectId) { // magic behavior for ObjectId
            instanceNode.put(key, ((ObjectId) value).toStringForm());
        } else if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;

            if (!collection.isEmpty() && collection.iterator().next() instanceof ObjectId) {
                ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();

                for (Object c : collection) {
                    if (c instanceof ObjectId) { // magic behavior for ObjectId
                        arrayNode.add(((ObjectId) c).toStringForm());
                    } else {
                        throw new RuntimeException("Unexpected type in collection of object id: " + c.getClass().getName());
                    }
                }
                instanceNode.put(key, arrayNode);
            } else {
                doSimpleSerializationPut(key, value);
            }
        } else {
            doSimpleSerializationPut(key, value);
        }

        return this;
    }

    public EventBuilder setTrackEventProcessedLifecycle(Boolean trackEventProcessedLifecycle) {
        JSON_EVENT_CONVENTIONS.setTrackEventProcessedLifecycle(eventNode, trackEventProcessedLifecycle);
        return this;
    }

    public Event build() {
        return new Event(eventNode, objectId);
    }

    private void doSimpleSerializationPut(String key, Object value) {
        JsonNode node = OBJECT_MAPPER.convertValue(value, JsonNode.class);
        instanceNode.put(key, node);
    }
}
/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.event.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class JsonEventConventions {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();

    public JsonEventConventions() {
    }

    public ObjectId getInstanceObjectId(ObjectNode eventNode) {
        return getInstanceObjectId(eventNode, getInstanceClassName(eventNode));
    }

    public ObjectId getInstanceObjectId(ObjectNode eventNode, String className) {
        return new ObjectId(className, getInstanceId(eventNode, className));
    }

    public String getInstanceClassName(ObjectNode eventNode) {
        int idx = 0;
        for (Iterator<String> fieldNames = eventNode.fieldNames(); fieldNames.hasNext();) {
            String fieldName = fieldNames.next();
            if (idx++ < ReservedFields.EVENT_FIELD_COUNT) {
                if (!fieldName.equals(ReservedFields.EVENT_ID)
                    && !fieldName.equals(ReservedFields.TENANT_ID)
                    && !fieldName.equals(ReservedFields.USER_ID)
                    && !fieldName.equals(ReservedFields.ACTOR_ID)
                    && !fieldName.equals(ReservedFields.CAUSED_BY)
                    && !fieldName.equals(ReservedFields.ACTIVITY_VERB)
                    && !fieldName.equals(ReservedFields.MODEL_VERSION_ID)
                    && !fieldName.equals(ReservedFields.NIL_FIELD)
                    && !fieldName.equals(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE)) {
                    return fieldName;
                }
            } else {
                Iterator<String> allFields = eventNode.fieldNames();
                List<String> annoying = Lists.newArrayList();
                while (allFields.hasNext()) {
                    annoying.add(allFields.next());
                }
                throw new IllegalArgumentException("Supplied event node has an unexpected set of fields: " + Arrays.toString(annoying.toArray()));
            }
        }

        return null;
    }

    public Id getInstanceId(ObjectNode eventNode, String className) {
        JsonNode got = eventNode.get(className);
        if (got == null || got.isNull() || !got.isObject()) {
            return null;
        }
        ObjectNode instanceNode = (ObjectNode) got;

        got = instanceNode.get(ReservedFields.INSTANCE_ID);
        if (got == null || got.isNull()) {
            return null;
        }
        try {
            return new Id(coder.decode(getTextValue(got)));
        } catch (Exception ex) {
            LOG.debug("Failed to get instanceId for className '" + className + "': " + eventNode, ex);
            return null;
        }
    }

    public TenantId getTenantId(ObjectNode eventNode) {
        try {
            JsonNode got = eventNode.get(ReservedFields.TENANT_ID);
            if (got == null || got.isNull()) {
                return null;
            }
            return new TenantId(got.textValue());
        } catch (Exception ex) {
            LOG.debug("Failed to get tenantId: " + eventNode, ex);
            return null;
        }
    }

    public long getEventId(ObjectNode eventNode) {
        JsonNode got = eventNode.get(ReservedFields.EVENT_ID);
        if (got == null || got.isNull()) {
            return 0L;
        }
        try {
            return got.longValue();
        } catch (Exception ex) {
            LOG.debug("Failed to get eventId: " + eventNode, ex);
            return 0L;
        }
    }

    public EventVerb getActivityVerb(ObjectNode eventNode) {
        return getActivityVerb(eventNode, ReservedFields.ACTIVITY_VERB);
    }

    public Id getActor(ObjectNode eventNode) {
        return getId(eventNode, ReservedFields.ACTOR_ID);
    }

    public Id getUserId(ObjectNode eventNode) {
        return getId(eventNode, ReservedFields.USER_ID);
    }

    public boolean hasCausedBy(ObjectNode eventNode) {
        return eventNode.has(ReservedFields.CAUSED_BY);
    }

    public long getCausedBy(ObjectNode eventNode) {
        return eventNode.get(ReservedFields.CAUSED_BY).longValue();
    }

    public boolean hasInstanceField(ObjectNode eventNode, String className, String fieldname) {
        ObjectNode objectNode = getInstanceNode(eventNode, className);
        if (objectNode != null) {
            return objectNode.has(fieldname);
        } else {
            return false;
        }
    }

    public JsonNode getInstanceField(ObjectNode eventNode, String className, String fieldName) {
        ObjectNode objectNode = getInstanceNode(eventNode, className);
        if (objectNode != null) {
            JsonNode got = objectNode.get(fieldName);
            if (got == null || got.isNull()) {
                return null;
            }
            return got;
        } else {
            return null;
        }
    }

    private String getTextValue(JsonNode got) {
        String gotTextValue = got.textValue();
        if (gotTextValue == null || gotTextValue.length() == 0) {
            throw new IllegalArgumentException("stringForm can not be null and must be at least 1 or more chars." + gotTextValue);
        }
        return gotTextValue;
    }

    private Id getId(ObjectNode objectNode, String fieldName) {
        JsonNode got = objectNode.get(fieldName);
        if (got == null || got.isNull()) {
            return null;
        }
        try {
            return new Id(coder.decode(getTextValue(got)));
        } catch (Exception ex) {
            LOG.debug("Failed to get objectId for field " + fieldName + ": " + objectNode, ex);
            return null;
        }
    }

    private EventVerb getActivityVerb(ObjectNode objectNode, String fieldName) {
        JsonNode got = objectNode.get(fieldName);
        if (got == null || got.isNull()) {
            return null;
        }
        try {
            return mapper.convertValue(got, EventVerb.class);
        } catch (Exception ex) {
            LOG.debug("Failed to get activity verb for field " + fieldName + ": " + objectNode, ex);
            return null;
        }
    }

    public ObjectNode getInstanceNode(ObjectNode eventNode, String className) {
        JsonNode got = eventNode.get(className);
        if (got == null || got.isNull()) {
            return null;
        }

        if (got.isObject()) {
            return (ObjectNode) got;
        } else {
            throw new IllegalArgumentException("Supplied event node has no instance node corresponding to " + className + ": " + eventNode);
        }

    }

    public void setInstanceNode(ObjectNode eventNode, String className, ObjectNode instanceNode) {
        eventNode.put(className, instanceNode);
    }

    public ObjectId getRef(ObjectNode eventNode, String fieldName) {
        return toRef(eventNode.get(fieldName));
    }

    public Id toId(JsonNode jsonNode) {
        if (jsonNode != null) {
            try {
                return mapper.convertValue(jsonNode, Id.class);
            } catch (Exception ex) {
                LOG.debug("Failed to get id: " + jsonNode, ex);
            }
        }

        return null;
    }

    public ObjectId toRef(JsonNode refNode) {
        if (refNode != null) {
            try {
                return mapper.convertValue(refNode, ObjectId.class);
            } catch (Exception ex) {
                LOG.debug("Failed to get objectId: " + refNode, ex);
            }
        }

        return null;
    }

    public List<ObjectId> toRefs(JsonNode refsNode) {
        if (refsNode != null) {
            try {
                return mapper.convertValue(refsNode, new TypeReference<List<ObjectId>>() {
                });
            } catch (Exception ex) {
                LOG.debug("Failed to get objectIds: " + refsNode, ex);
            }
        }

        return null;
    }

    public void setTenantId(ObjectNode event, TenantId tenantId) {
        event.put(ReservedFields.TENANT_ID, mapper.valueToTree(tenantId));
    }

    public ObjectNode setInstanceClassName(ObjectNode event, String className) {
        ObjectNode instanceNode = mapper.createObjectNode();
        event.put(className, instanceNode);

        return instanceNode;
    }

    public void setInstanceId(ObjectNode event, Id id, String className) {
        JsonNode got = event.get(className);
        if (got == null || got.isNull() || !got.isObject()) {
            throw new IllegalArgumentException("Supplied event node has no instance node corresponding to " + className);
        }

        ObjectNode instanceNode = (ObjectNode) got;
        instanceNode.put(ReservedFields.INSTANCE_ID, id.toStringForm());
    }

    public void setEventId(ObjectNode event, long id) {
        event.put(ReservedFields.EVENT_ID, id);
    }

    public void setActorId(ObjectNode event, Id id) {
        event.put(ReservedFields.ACTOR_ID, id.toStringForm());
    }

    public void setUserId(ObjectNode event, Id id) {
        event.put(ReservedFields.USER_ID, id.toStringForm());
    }

    public void setCausedBy(ObjectNode event, long eventId) {
        event.put(ReservedFields.CAUSED_BY, eventId);
    }

    public void setActivityVerb(ObjectNode event, EventVerb eventVerb) {
        event.put(ReservedFields.ACTIVITY_VERB, eventVerb.name());
    }

    public void validate(ObjectNode event) {
        Preconditions.checkNotNull(event);
        TenantId tenantId = getTenantId(event);
        int expectedSize = 4;
        if (tenantId == null) {
            throw new IllegalArgumentException("Event is missing tenant id");
        }
        Id actorId = getActor(event);
        if (actorId == null) {
            throw new IllegalArgumentException("Event has missing or invalid actorId:" + actorId);
        }
        Id userId = getUserId(event);
        if (userId == null) {
            throw new IllegalArgumentException("Event has missing or invalid userId:" + userId);
        }
        if (event.has(ReservedFields.CAUSED_BY)) {
            expectedSize++;
        }
        if (event.has(ReservedFields.ACTIVITY_VERB)) {
            expectedSize++;
        }
        if (event.has(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE)) {
            expectedSize++;
        }
        if (event.has(ReservedFields.EVENT_ID)) {
            expectedSize++;
        }
        if (event.has(ReservedFields.TRACE)) {
            expectedSize++;
        }
        String className = Strings.nullToEmpty(getInstanceClassName(event)).trim();
        if (event.has(ReservedFields.NIL_FIELD) || hasInstanceField(event, className, ReservedFields.NIL_FIELD)) {
            throw(new IllegalArgumentException("Nil field may never be emitted"));
        }
        if (Strings.isNullOrEmpty(className)) {
            throw new IllegalArgumentException("Event is missing payload");
        }
        Id instanceId = getInstanceId(event, className);
        if (instanceId == null) {
            throw new IllegalArgumentException("Event is missing instanceId field");
        }
        if (event.size() != expectedSize) {
            throw new IllegalArgumentException("Event does not have the expected number of top level fields. " + event.size() + "!=" + expectedSize);
        }
    }

    public void setTrackEventProcessedLifecycle(ObjectNode eventNode, Boolean trackEventProcessedLifecycle) {
        if (trackEventProcessedLifecycle == null) {
            eventNode.remove(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE);
        } else {
            eventNode.put(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE, trackEventProcessedLifecycle);
        }
    }

    public boolean isTrackEventProcessedLifecycle(ObjectNode eventNode) {
        if (eventNode.has(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE)) {
            return eventNode.get(ReservedFields.TRACK_EVENT_PROCESSED_LIFECYCLE).booleanValue();
        } else {
            return false;
        }
    }
}

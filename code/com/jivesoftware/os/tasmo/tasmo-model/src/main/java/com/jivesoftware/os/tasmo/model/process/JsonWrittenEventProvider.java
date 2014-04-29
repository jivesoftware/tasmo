package com.jivesoftware.os.tasmo.model.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * JSON based implementation of WrittenEventProvider.
 */
public class JsonWrittenEventProvider implements WrittenEventProvider<ObjectNode, JsonNode> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonEventConventions eventHelper = new JsonEventConventions();

    @Override
    public WrittenEvent convertEvent(ObjectNode eventData) {
        JsonWrittenEvent event = new JsonWrittenEvent(eventData, eventHelper);
        validate(event);
        return event;
    }

    @Override
    public TypeMarshaller<OpaqueFieldValue> getLiteralFieldValueMarshaller() {
        return new TypeMarshaller<OpaqueFieldValue>() {

            @Override
            public JsonLiteralFieldValue fromBytes(byte[] bytes) throws Exception {
                JsonNode node = mapper.readValue(new ByteArrayInputStream(bytes), JsonNode.class);
                return new JsonLiteralFieldValue(node);
            }

            @Override
            public byte[] toBytes(OpaqueFieldValue j) throws Exception {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                mapper.writeValue(output, ((JsonLiteralFieldValue) j).fieldVal);
                return output.toByteArray();
            }

            @Override
            public JsonLiteralFieldValue fromLexBytes(byte[] lexBytes) throws Exception {
                return fromBytes(lexBytes);
            }

            @Override
            public byte[] toLexBytes(OpaqueFieldValue t) throws Exception {
                return toBytes(t);
            }
        };
    }

    @Override
    public OpaqueFieldValue convertFieldValue(JsonNode fieldValue) {
        return new JsonLiteralFieldValue(fieldValue);
    }

    @Override
    public LeafNodeFields createLeafNodeFields() {
        return new JsonLeafNodeFields(mapper);
    }

    private static class JsonWrittenEvent implements WrittenEvent {

        private final ObjectNode eventNode;
        private final JsonEventConventions eventConventions;
        private final JsonEventPayload payload;
        private final boolean isBookKeepingEnabled;

        public JsonWrittenEvent(ObjectNode eventNode, JsonEventConventions eventConventions) {
            this.eventNode = eventNode;
            this.eventConventions = eventConventions;
            this.isBookKeepingEnabled = eventConventions.isTrackEventProcessedLifecycle(eventNode);
            String className = eventConventions.getInstanceClassName(eventNode);
            ObjectNode instanceNode = eventConventions.getInstanceNode(eventNode, className);
            this.payload = new JsonEventPayload(instanceNode, eventConventions.getInstanceObjectId(eventNode, className), eventConventions);
        }


        @Override
        public Optional<String> getCorrelationId() {
            return Optional.absent();
        }

        @Override
        public TenantId getTenantId() {
            return eventConventions.getTenantId(eventNode);
        }

        @Override
        public Id getActorId() {
            return eventConventions.getActor(eventNode);
        }

        @Override
        public long getEventId() {
            return eventConventions.getEventId(eventNode);
        }

        @Override
        public Id getCentricId() {
            return eventConventions.getUserId(eventNode);
        }

        @Override
        public WrittenInstance getWrittenInstance() {
            return payload;
        }

        @Override
        public boolean isBookKeepingEnabled() {
            return isBookKeepingEnabled;
        }

        @Override
        public String toString() {
            return "JsonWrittenEvent{" + "eventNode=" + eventNode
                    + '}';
        }

    }

    private static void validate(JsonWrittenEvent toValidate) {
        Preconditions.checkNotNull(toValidate.getActorId());
        Preconditions.checkNotNull(toValidate.getTenantId());

        WrittenInstance payload = Preconditions.checkNotNull(toValidate.getWrittenInstance());
        Preconditions.checkNotNull(payload.getInstanceId());
    }

    private static class JsonEventPayload implements WrittenInstance {

        final ObjectNode instanceNode;
        final ObjectId instanceId;
        final JsonEventConventions eventConventions;

        public JsonEventPayload(ObjectNode instanceNode, ObjectId instanceId, JsonEventConventions eventConventions) {
            this.instanceNode = instanceNode;
            this.eventConventions = eventConventions;
            this.instanceId = instanceId;
        }

        @Override
        public Iterable<String> getFieldNames() {
            return Lists.newArrayList(instanceNode.fieldNames());
        }

        @Override
        public OpaqueFieldValue getFieldValue(String fieldName) {
            return new JsonLiteralFieldValue(instanceNode.get(fieldName));
        }

        @Override
        public void removeField(String fieldName) {
            instanceNode.remove(fieldName);
        }

        @Override
        public ObjectId getInstanceId() {
            return instanceId;
        }

        @Override
        public ObjectId[] getMultiReferenceFieldValue(String fieldName) {
            JsonNode refVal = instanceNode.get(fieldName);
            List<ObjectId> multiRefs = eventConventions.toRefs(refVal);
            if (multiRefs != null) {
                return multiRefs.toArray(new ObjectId[multiRefs.size()]);
            } else {
                return null;
            }
        }

        @Override
        public boolean hasField(String fieldName) {
            return instanceNode.has(fieldName);
        }

        @Override
        public boolean isDeletion() {
            JsonNode deletion = instanceNode.get(ReservedFields.DELETED);
            if (deletion != null && deletion.isBoolean()) {
                return ((BooleanNode) deletion).booleanValue();
            }

            return false;
        }

        @Override
        public ObjectId getReferenceFieldValue(String fieldName) {
            JsonNode refVal = instanceNode.get(fieldName);
            return eventConventions.toRef(refVal);
        }

        @Override
        public Id getIdFieldValue(String fieldName) {
            JsonNode refVal = instanceNode.get(fieldName);
            return eventConventions.toId(refVal);
        }
    }

    private static class JsonLiteralFieldValue implements OpaqueFieldValue {

        final JsonNode fieldVal;

        public JsonLiteralFieldValue(JsonNode fieldVal) {
            this.fieldVal = fieldVal;
        }

        @Override
        public boolean isNull() {
            return fieldVal == null || fieldVal.isNull();
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 83 * hash + Objects.hashCode(this.fieldVal);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final JsonLiteralFieldValue other = (JsonLiteralFieldValue) obj;
            if (!Objects.equals(this.fieldVal, other.fieldVal)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "JsonLiteralFieldValue{" + "fieldVal=" + fieldVal + '}';
        }
    }

    private static class JsonLeafNodeFields implements LeafNodeFields {

        private final ObjectMapper mapper;
        private final ObjectNode fieldsNode;

        public JsonLeafNodeFields(ObjectMapper mapper) {
            this.mapper = mapper;
            this.fieldsNode = mapper.createObjectNode();
        }

        @Override
        public void addField(String fieldName, OpaqueFieldValue value) {
            fieldsNode.put(fieldName, ((JsonLiteralFieldValue) value).fieldVal);
        }

        @Override
        public void removeField(String fieldName) {
            fieldsNode.remove(fieldName);
        }

        @Override
        public OpaqueFieldValue getField(String fieldName) {
            JsonNode val = fieldsNode.get(fieldName);
            return val != null ? new JsonLiteralFieldValue(val) : null;
        }

        @Override
        public boolean hasField(String fieldName) {
            return fieldsNode.has(fieldName);
        }

        @Override
        public String toStringForm() throws IOException {
            return mapper.writeValueAsString(fieldsNode);
        }

        @Override
        public boolean hasFields() {
            return fieldsNode.size() > 0;
        }

        @Override
        public String toString() {
            try {
                return "JsonLeafNodeFields{" + "fieldsNode=" + mapper.writeValueAsString(fieldsNode) + '}';
            } catch (JsonProcessingException ex) {
                return super.toString();
            }
        }

        @Override
        public void addBooleanField(String fieldName, boolean value) {
            fieldsNode.put(fieldName, value);
        }

        @Override
        public Boolean getBooleanField(String fieldname) {
            JsonNode value = fieldsNode.get(fieldname);
            if (value instanceof BooleanNode) {
                return value.booleanValue();
            } else {
                return null;
            }
        }
    }
}

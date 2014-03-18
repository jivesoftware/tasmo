package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventModel {

    private final String eventClass;
    private final Map<String, ValueType> eventFields;

    public EventModel(String eventClass, Map<String, ValueType> eventFields) {
        this.eventClass = eventClass;
        this.eventFields = eventFields;
        this.eventFields.put(ReservedFields.INSTANCE_ID, ValueType.value);
    }

    public String getEventClass() {
        return eventClass;
    }

    public Map<String, ValueType> getEventFields() {
        return eventFields;
    }

    private boolean fieldIsStable(String fieldName) {
        //TODO implement stable field configuration - until then no fields are stable and anything goes
        return false;
    }

    /**
     * @param other
     * @return true if other event model is compatible.
     * @throws ModelMasterException
     */
    public void assertEventIsCompatible(EventModel other) throws Exception {
        Map<String, ValueType> theseFields = this.getEventFields();
        Map<String, ValueType> otherFields = other.getEventFields();

        for (String fieldName : theseFields.keySet()) {
            ValueType otherFieldType = otherFields.get(fieldName);
            if (fieldIsStable(fieldName) && otherFieldType == null) {
                throw new Exception("You cannot remove a stable field once it has been added. Field:" + fieldName);
            } else {
                ValueType fieldType = theseFields.get(fieldName);
                if (fieldIsStable(fieldName) && !fieldType.equals(otherFieldType)) {
                    throw new Exception("You cannot change a stable field's type once it has been added. Field:"
                            + fieldName + " " + otherFieldType);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "EventConfiguration{" + "eventClass=" + eventClass + ", eventFields=" + eventFields + '}';
    }

    public static EventModelBuilder builder(ObjectNode exampleViewNode, boolean isExampleEvent) {
        return new EventModelBuilder(exampleViewNode, isExampleEvent);
    }

    public static class EventModelBuilder {

        private final ObjectNode eventNode;
        private final JsonEventConventions eventHelper = new JsonEventConventions(); // barf
        private final boolean isExampleEvent;

        private EventModelBuilder(ObjectNode eventNode, boolean isExampleEvent) {
            this.eventNode = eventNode;
            this.isExampleEvent = isExampleEvent;
        }

        public EventModel build() {
            String eventClass = eventHelper.getInstanceClassName(eventNode);
            Map<String, ValueType> eventFields = new HashMap<>();

            ObjectNode instanceNode = eventHelper.getInstanceNode(eventNode, eventClass);

            if (instanceNode == null || instanceNode.isNull()) {
                throw new IllegalArgumentException("Events must contain a populated instance node");
            }

            for (Iterator<String> eventFieldNames = instanceNode.fieldNames(); eventFieldNames.hasNext();) {
                String eventFieldName = eventFieldNames.next();
                JsonNode exampleValue = instanceNode.get(eventFieldName);

                if (isExampleEvent && (exampleValue == null || exampleValue.isNull())) {
                    throw new IllegalArgumentException("Example event fields must contain a valid value. " + eventFieldName + " does not");
                }

                if (exampleValue.isTextual() && ObjectId.isStringForm(exampleValue.textValue())) {
                    eventFields.put(eventFieldName, ValueType.ref);
                } else if (exampleValue.isArray()) {
                    ArrayNode exampleArray = (ArrayNode) exampleValue;
                    if (isExampleEvent && (exampleArray.size() != 1)) {
                        throw new IllegalArgumentException("Array values in example events must contain one element. "
                                + "The array value for field " + eventFieldName + " has " + exampleArray.size() + " elements.");
                    }

                    if (exampleArray.size() > 0) {
                        JsonNode element = exampleArray.get(0);

                        if (element.isNull()) {
                            if (isExampleEvent) {
                                throw new IllegalArgumentException("Elements in event field value arrays can't be null. "
                                        + "The " + eventFieldName + " field holds an array with an empty json node as the sole element.");
                            } else {
                                eventFields.put(eventFieldName, ValueType.unknown);
                            }
                        } else {
                            if (element.isTextual() && ObjectId.isStringForm(element.textValue())) {
                                eventFields.put(eventFieldName, ValueType.refs);
                            } else {
                                eventFields.put(eventFieldName, ValueType.value);
                            }
                        }
                    } else {
                        eventFields.put(eventFieldName, ValueType.unknown);
                    }

                } else {
                    eventFields.put(eventFieldName, ValueType.value);
                }
            }
            return new EventModel(eventClass, eventFields);
        }
    }
}

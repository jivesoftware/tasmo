/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class JsonViewMerger {

    private final ObjectMapper mapper;

    public JsonViewMerger(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    //this updated main node in place and returns it as well
    public void merge(ObjectNode mainNode, ObjectNode updateNode) {
        Iterator<String> fieldNames = updateNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode updateFieldValue = updateNode.get(fieldName);
            JsonNode existingFieldValue = mainNode.get(fieldName);
            if (updateFieldValue != null) {
                if (existingFieldValue == null) {
                    mainNode.put(fieldName, updateFieldValue);
                } else if (updateFieldValue.getClass() == existingFieldValue.getClass()) {
                    if (updateFieldValue.isArray()) {
                        ArrayNode mergedArrays = mergeArrays((ArrayNode) existingFieldValue, (ArrayNode) updateFieldValue);
                        mainNode.put(fieldName, mergedArrays);
                    } else if (updateFieldValue.isObject()) {
                        merge((ObjectNode) existingFieldValue, (ObjectNode) updateFieldValue);
                    } else {
                        //when primitive values, gotta do something :) /////
                        mainNode.put(fieldName, updateFieldValue);
                    }
                } else {
                    throw new IllegalArgumentException("Mismatchd json types for field named "
                        + fieldName + ": " + existingFieldValue.getClass().getSimpleName() + " != " + updateFieldValue.getClass().getSimpleName());
                }
            }
        }
    }

    //this does not modify existing array in place - just returns result
    public ArrayNode mergeArrays(ArrayNode existing, ArrayNode update) {
        if (update.size() == 0) {
            return existing;
        }
        if (existing.size() == 0) {
            return update;
        }

        ArrayNode merged = mapper.createArrayNode();
        Iterator<JsonNode> existingElements = existing.elements();
        Iterator<JsonNode> updateElements = update.elements();
        JsonNode existingElement = existingElements.next();
        JsonNode updateElement = updateElements.next();

        while (true) {
            JsonNode aIdValue = existingElement.get(FieldConstants.VIEW_OBJECT_ID);
            JsonNode bIdValue = updateElement.get(FieldConstants.VIEW_OBJECT_ID);

            if (aIdValue == null) {
                if (bIdValue == null) {
                    //we are a leaf node primitive value array being merged over itself
                    //due to two identical model paths in the same view definition
                    return update;
                } else {
                    throw new IllegalArgumentException("Object array merges require contents to have object ids");
                }
            } else if (bIdValue == null) {
                throw new IllegalArgumentException("Object array merges require contents to have object ids");
            }

            ObjectId aid = new ObjectId(aIdValue.textValue());
            ObjectId bid = new ObjectId(bIdValue.textValue());

            int compared = compare(aid, bid);
            // merge
            if (compared == 0) {
                merge((ObjectNode) existingElement, (ObjectNode) updateElement);
                merged.add(existingElement);
                if (existingElements.hasNext() && updateElements.hasNext()) {
                    existingElement = existingElements.next();
                    updateElement = updateElements.next();
                } else {
                    break;
                }

            } else if (compared < 0) {
                // insert
                merged.add(updateElement);
                if (updateElements.hasNext()) {
                    updateElement = updateElements.next();
                } else {
                    merged.add(existingElement);
                    break;
                }

            } else {
                // insert
                merged.add(existingElement);
                if (existingElements.hasNext()) {
                    existingElement = existingElements.next();
                } else {
                    merged.add(updateElement);
                    break;
                }
            }
        }

        while (existingElements.hasNext()) {
            merged.add(existingElements.next());
        }

        while (updateElements.hasNext()) {
            merged.add(updateElements.next());
        }

        return merged;
    }

    public int compare(ObjectId aId, ObjectId bId) {
        if (aId.equals(bId)) {
            return 0;
        }
        return -(aId.getId().compareTo(bId.getId()));
    }

    public ObjectNode toObjectNode(String jsonString) throws IOException {
        if (jsonString == null) {
            return mapper.createObjectNode();
        }
        if (!jsonString.startsWith(JsonToken.START_OBJECT.asString())) {
            jsonString = mapper.readValue(jsonString, String.class);
        }

        return mapper.readValue(jsonString, ObjectNode.class);
    }

    public ArrayNode createArrayNode() {
        return mapper.createArrayNode();
    }

    public ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }
}

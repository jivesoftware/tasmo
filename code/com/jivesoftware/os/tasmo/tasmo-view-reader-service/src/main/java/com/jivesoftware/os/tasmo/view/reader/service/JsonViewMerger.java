package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

/**
 *
 */
public class JsonViewMerger {

    private final ObjectMapper mapper;

    public JsonViewMerger(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode toObjectNode(byte[] jsonBytes) throws IOException {
        if (jsonBytes == null) {
            return mapper.createObjectNode();
        }
        return mapper.readValue(jsonBytes, ObjectNode.class);
    }

    public ArrayNode createArrayNode() {
        return mapper.createArrayNode();
    }

    public ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }
}

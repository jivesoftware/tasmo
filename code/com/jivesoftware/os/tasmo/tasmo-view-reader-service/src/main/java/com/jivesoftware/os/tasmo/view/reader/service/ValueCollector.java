/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.io.IOException;

/**
 *
 */
class ValueCollector implements Collector {

    private final JsonViewMerger merger;
    private final String className;
    private ObjectNode valueObject;

    public ValueCollector(JsonViewMerger merger, String className) {
        // Mappers have serializer caches that should be shared between fields, as creating the serializers by far dominates usages of ObjectMapper
        this.merger = merger;
        this.className = className;
    }

    @Override
    public void process(Collector[] collectors, int i, Id orderId, String value, long timestamp) throws IOException {
        valueObject = merger.toObjectNode(value);
        valueObject.put(FieldConstants.VIEW_OBJECT_ID, new ObjectId(className, orderId).toStringForm());
        if (i > 0) {
            collectors[i - 1].link(valueObject, timestamp);
        }
    }

    @Override
    public void link(JsonNode value, long timestamp) {
        throw new UnsupportedOperationException("Can't link from downstream of a value node");
    }

    @Override
    public ObjectNode active() {
        return valueObject;
    }
}

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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;

/**
 *
 */
class RefsCollector implements Collector {

    private final JsonViewMerger merger;
    private final String className;
    private final String fieldName;
    private ArrayNode activeCollector;
    private Id activeOrderId = null;

    public RefsCollector(JsonViewMerger merger, String className, String fieldName) {
        // Mappers have serializer caches that should be shared between fields, as creating the serializers by far dominates usages of ObjectMapper
        this.merger = merger;
        this.className = className;
        this.fieldName = fieldName;
    }

    @Override
    public void process(Collector[] collectors, int i, Id orderId, String value, long timestamp) {
        if (activeOrderId == null || !orderId.equals(activeOrderId)) {
            activeCollector = merger.createArrayNode();

            if (i > 0) {
                ObjectNode collectionMap = merger.createObjectNode();
                collectionMap.put(FieldConstants.VIEW_OBJECT_ID, new ObjectId(className, orderId).toStringForm());
                collectionMap.put(fieldName, activeCollector);
                collectors[i - 1].link(collectionMap, timestamp);
            }
            activeOrderId = orderId;
        }
    }

    @Override
    public void link(JsonNode value, long timestamp) {
        if (activeCollector == null) {
            activeCollector = merger.createArrayNode();
        }
        activeCollector.add(value);
    }

    @Override
    public ObjectNode active() {
        ObjectNode wrapper = merger.createObjectNode();
        wrapper.put(fieldName, activeCollector);
        wrapper.put(FieldConstants.VIEW_OBJECT_ID, new ObjectId(className, activeOrderId).toStringForm());
        return wrapper;
    }
}

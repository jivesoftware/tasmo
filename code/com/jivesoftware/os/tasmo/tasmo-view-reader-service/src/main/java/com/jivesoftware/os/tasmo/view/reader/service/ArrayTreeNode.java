/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ArrayTreeNode {

    private final Map<ObjectId, MapTreeNode> array = new LinkedHashMap<>();

    public ArrayTreeNode() {
    }

    public Collection<MapTreeNode> values() {
        return array.values();
    }

    public void add(ModelPathStep[] steps, ObjectId[] ids,  ViewValue value, Long threadTimestamp) {
       ObjectId objectId = ids[0];
        MapTreeNode child = array.get(objectId);
        if (child == null) {
            child = new MapTreeNode(objectId);
            array.put(objectId, child);
        }
        child.add(steps, ids, value, threadTimestamp);
    }

    public JsonNode merge(JsonViewMerger merger, Set<Id> permittedIds) throws IOException {
        ArrayNode arrayNode = merger.createArrayNode();
        for (MapTreeNode treeNode : array.values()) {
            if (permittedIds.contains(treeNode.getObjectId().getId())) {
                arrayNode.add(treeNode.merge(merger, permittedIds));
            }
        }
        return arrayNode;
    }
}


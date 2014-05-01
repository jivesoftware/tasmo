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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;

/**
 *
 */
class LatestTreeNode implements MultiTreeNode {

    private final ArrayTreeNode arrayTreeNode;

    public LatestTreeNode(ArrayTreeNode arrayTreeNode) {
        this.arrayTreeNode = arrayTreeNode;
    }

    @Override
    public void add(ModelPathStep[] steps, ObjectId[] ids, ViewValue value, Long threadTimestamp) {
        arrayTreeNode.add(steps, ids, value, threadTimestamp);
    }

    @Override
    public JsonNode merge(JsonViewMerger merger, final Set<Id> permittedIds) throws IOException {
        Ordering<MapTreeNode> timestampOrdering = Ordering.from(new Comparator<MapTreeNode>() {
            @Override
            public int compare(MapTreeNode o1, MapTreeNode o2) {
                long diff = o1.getHighWaterTimestamp() - o2.getHighWaterTimestamp();
                return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
            }
        });

        Iterable permitted = Iterables.filter(arrayTreeNode.values(), new Predicate<MapTreeNode>() {
            @Override
            public boolean apply(MapTreeNode input) {
                return permittedIds.contains(input.getObjectId().getId());
            }
        });

        MapTreeNode latestTreeNode = null;
        if (permitted.iterator().hasNext()) {
            latestTreeNode = timestampOrdering.max(permitted);
        }

        if (latestTreeNode == null) {
            return null;
        }
        return latestTreeNode.merge(merger, permittedIds);
    }

    @Override
    public String getFieldPrefix() {
        return ReservedFields.LATEST_BACK_REF_FIELD_PREFIX;
    }

}


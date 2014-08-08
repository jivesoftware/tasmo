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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class MapTreeNode implements TreeNode {

    private final ObjectId objectId;
    private final Collection<ViewValue> values = new ArrayList<>();
    private final Map<String, MapTreeNode> singleChildren = new HashMap<>();
    private final Map<StepTypeAndFieldName, MultiTreeNode> multiChildren = new HashMap<>();
    private Long threadTimestamp;
    private long[] modelPathTimestamps;

    public MapTreeNode(ObjectId objectId) {
        this.objectId = objectId;
    }

    @Override
    public void add(ModelPathStep[] steps, ObjectId[] ids, ViewValue value, Long threadTimestamp) {
        this.modelPathTimestamps = value.getModelPathTimeStamps();
        this.threadTimestamp = threadTimestamp;
        ModelPathStep thisStep = steps[0];
        switch (thisStep.getStepType()) {
            case value:
            case centric_value:
                values.add(value);
                break;
            case ref:
            case centric_ref: {
                String refFieldName = thisStep.getRefFieldName();
                MapTreeNode child = singleChildren.get(refFieldName);
                if (child == null) {
                    child = new MapTreeNode(ids[1]);
                    singleChildren.put(refFieldName, child);
                }
                child.add(Arrays.copyOfRange(steps, 1, steps.length), Arrays.copyOfRange(ids, 1, ids.length), value, threadTimestamp);
                break;
            }
            default: {
                StepTypeAndFieldName stepTypeAndFieldName = new StepTypeAndFieldName(thisStep.getStepType(), thisStep.getRefFieldName());
                MultiTreeNode treeNode = multiChildren.get(stepTypeAndFieldName);
                if (treeNode == null) {
                    switch (thisStep.getStepType()) {
                        case refs:
                        case centric_refs:
                            treeNode = new AllForwardTreeNode(new ArrayTreeNode());
                            break;
                        case backRefs:
                        case centric_backRefs:
                            treeNode = new AllBackTreeNode(new ArrayTreeNode());
                            break;
                        case latest_backRef:
                        case centric_latest_backRef:
                            treeNode = new LatestTreeNode(new ArrayTreeNode());
                            break;
                        case count:
                        case centric_count:
                            treeNode = new CountTreeNode();
                            break;
                    }
                    multiChildren.put(stepTypeAndFieldName, treeNode);
                }
                treeNode.add(Arrays.copyOfRange(steps, 1, steps.length), Arrays.copyOfRange(ids, 1, ids.length), value, threadTimestamp);
                break;
            }
        }
    }

    @Override
    public JsonNode merge(JsonViewMerger merger, Set<Id> permittedIds) throws IOException {
        ObjectNode objectNode = merger.createObjectNode();
        objectNode.put(ReservedFields.VIEW_OBJECT_ID, objectId.toStringForm());
        for (ViewValue value : values) {
            objectNode.putAll(merger.toObjectNode(value.getValue()));
        }
        for (Map.Entry<String, MapTreeNode> entry : singleChildren.entrySet()) {
            if (permittedIds.contains(entry.getValue().getObjectId().getId())) {
                objectNode.put(entry.getKey(), entry.getValue().merge(merger, permittedIds));
            }
        }
        for (Map.Entry<StepTypeAndFieldName, MultiTreeNode> entry : multiChildren.entrySet()) {
            MultiTreeNode treeNode = entry.getValue();
            if (treeNode instanceof CountTreeNode) {
                objectNode.put(treeNode.getFieldPrefix() + entry.getKey().fieldName, treeNode.merge(merger, permittedIds).get(CountTreeNode.JSON_FIELD));
            } else {
                objectNode.put(treeNode.getFieldPrefix() + entry.getKey().fieldName, treeNode.merge(merger, permittedIds));
            }
        }
        return objectNode;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public Long getThreadTimestamp() {
        return threadTimestamp;
    }

    public long[] getModelPathTimestamps() {
        return modelPathTimestamps;
    }

    private static class StepTypeAndFieldName {

        private final ModelPathStepType stepType;
        private final String fieldName;

        private StepTypeAndFieldName(ModelPathStepType stepType, String fieldName) {
            this.stepType = stepType;
            this.fieldName = fieldName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StepTypeAndFieldName that = (StepTypeAndFieldName) o;

            if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
                return false;
            }
            if (stepType != that.stepType) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = stepType != null ? stepType.hashCode() : 0;
            result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
            return result;
        }
    }
}

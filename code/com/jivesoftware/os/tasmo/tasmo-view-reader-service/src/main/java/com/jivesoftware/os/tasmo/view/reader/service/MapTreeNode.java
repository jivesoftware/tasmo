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
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
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
    private Collection<String> values = new ArrayList<>();
    private Map<String, MapTreeNode> singleChildren = new HashMap<>();
    private Map<StepTypeAndFieldName, MultiTreeNode> multiChildren = new HashMap<>();
    private long timestamp;

    public MapTreeNode(ObjectId objectId) {
        this.objectId = objectId;
    }

    @Override
    public void add(ModelPathStep[] steps, ObjectId[] ids, String value, Long timestamp) {
        ModelPathStep thisStep = steps[0];
        switch (thisStep.getStepType()) {
            case value:
                values.add(value);
                if (timestamp != null && timestamp > this.timestamp) {
                    this.timestamp = timestamp;
                }
                break;
            case ref: {
                String refFieldName = thisStep.getRefFieldName();
                MapTreeNode child = singleChildren.get(refFieldName);
                if (child == null) {
                    child = new MapTreeNode(ids[1]);
                    singleChildren.put(refFieldName, child);
                }
                child.add(Arrays.copyOfRange(steps, 1, steps.length), Arrays.copyOfRange(ids, 1, ids.length), value, timestamp);
                break;
            }
            default: {
                StepTypeAndFieldName stepTypeAndFieldName = new StepTypeAndFieldName(thisStep.getStepType(), thisStep.getRefFieldName());
                MultiTreeNode treeNode = multiChildren.get(stepTypeAndFieldName);
                if (treeNode == null) {
                    switch (thisStep.getStepType()) {
                        case refs:
                            treeNode = new AllForwardTreeNode();
                            break;
                        case backRefs:
                            treeNode = new AllBackTreeNode();
                            break;
                        case latest_backRef:
                            treeNode = new LatestTreeNode();
                            break;
                        case count:
                            treeNode = new CountTreeNode();
                            break;
                    }
                    multiChildren.put(stepTypeAndFieldName, treeNode);
                }
                treeNode.add(Arrays.copyOfRange(steps, 1, steps.length), Arrays.copyOfRange(ids, 1, ids.length), value, timestamp);
                break;
            }
        }
    }

    @Override
    public JsonNode merge(JsonViewMerger merger, Set<Id> permittedIds) throws IOException {
        ObjectNode objectNode = merger.createObjectNode();
        objectNode.put(ReservedFields.VIEW_OBJECT_ID, objectId.toStringForm());
        for (String value : values) {
            objectNode.putAll(merger.toObjectNode(value));
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

    public long getTimestamp() {
        return timestamp;
    }

    private static final class StepTypeAndFieldName {

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

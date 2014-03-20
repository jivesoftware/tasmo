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
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author pete
 */
public class JsonViewFormatter implements ViewFormatter<ObjectNode> {

    private final ObjectMapper mapper;
    private ObjectNode root;
    private List<JsonNode> level;
    private List<JsonNode> nextLevel;
    private static final String COUNT_PREFIX = "count_";
    private static final String ALL_PREFIX = "all_";
    private static final String LATEST_PREFIX = "latest_";

    public JsonViewFormatter(ObjectMapper mapper) {
        this.mapper = mapper;
        this.level = new ArrayList<>();
        this.nextLevel = new ArrayList<>();
    }

    @Override
    public void setRoot(ObjectId viewRoot) {
        root = mapper.createObjectNode();
        root.put(ReservedFields.VIEW_OBJECT_ID, viewRoot.toStringForm());
        level.add(root);
    }

    @Override
    public void nextLevel() {
        level.clear();
        level.addAll(nextLevel);
        nextLevel.clear();
    }

    @Override
    public void nextPath() {
        level.clear();
        level.add(root);
        nextLevel.clear();
    }

    @Override
    public ObjectNode getView() {
        if (root == null) {
            root = mapper.createObjectNode();
        }
        return root;
    }

    @Override
    public void addReferenceNode(ViewReference reference) {
        String viewObjectId = reference.getOriginId().toStringForm();
        ModelPathStepType stepType = reference.getStepType();

        for (JsonNode node : level) {
            if (node.isObject()) {

                ObjectNode object = (ObjectNode) node;
                if (viewObjectId.equals(object.get(ReservedFields.VIEW_OBJECT_ID).textValue())) {
                    addReferenceToObject(stepType, object, reference);
                }

            } else if (node.isArray()) {

                ArrayNode array = (ArrayNode) node;
                for (Iterator<JsonNode> iter = array.elements(); iter.hasNext();) {
                    JsonNode element = iter.next();
                    if (element.isObject()) {
                        ObjectNode object = (ObjectNode) element;
                        if (viewObjectId.equals(object.get(ReservedFields.VIEW_OBJECT_ID).textValue())) {
                            addReferenceToObject(stepType, object, reference);
                        }
                    } else {
                        throw new IllegalStateException("Unexpected array element type. Expected object, found " + element.getNodeType());
                    }
                }

            } else {
                throw new IllegalStateException("Unexpected node type " + node.getNodeType());
            }
        }

    }

    @Override
    public void addValueNode(ViewValue value) {
        if (root == null) {
            root = mapper.createObjectNode();
            root.put(ReservedFields.VIEW_OBJECT_ID, value.getObjectId().toStringForm());
            level.add(root);
        }

    }

    private void addReferenceToObject(ModelPathStepType stepType, ObjectNode object, ViewReference reference) {
        if (ModelPathStepType.count.equals(stepType)) {

            object.put(COUNT_PREFIX + reference.getRefFieldName(), reference.getCountValue());

        } else if (ModelPathStepType.latest_backRef.equals(stepType)) {

            ObjectId destination = reference.getDestinationIds().get(0);
            ObjectNode destinationNode = mapper.createObjectNode();
            destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

            object.put(LATEST_PREFIX + reference.getRefFieldName(), destinationNode);
            nextLevel.add(destinationNode);

        } else if (ModelPathStepType.backRefs.equals(stepType)) {

            ArrayNode array = mapper.createArrayNode();
            for (ObjectId destination : reference.getDestinationIds()) {
                ObjectNode destinationNode = mapper.createObjectNode();
                destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

                array.add(destinationNode);
            }
            object.put(ALL_PREFIX + reference.getRefFieldName(), array);
            nextLevel.add(array);

        } else if (ModelPathStepType.ref.equals(stepType)) {

            ObjectId destination = reference.getDestinationIds().get(0);
            ObjectNode destinationNode = mapper.createObjectNode();
            destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

            object.put(reference.getRefFieldName(), destinationNode);
            nextLevel.add(destinationNode);

        } else if (ModelPathStepType.refs.equals(stepType)) {

            ArrayNode array = mapper.createArrayNode();
            for (ObjectId destination : reference.getDestinationIds()) {
                ObjectNode destinationNode = mapper.createObjectNode();
                destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

                array.add(destinationNode);
            }
            object.put(reference.getRefFieldName(), array);
            nextLevel.add(array);

        }
    }
}

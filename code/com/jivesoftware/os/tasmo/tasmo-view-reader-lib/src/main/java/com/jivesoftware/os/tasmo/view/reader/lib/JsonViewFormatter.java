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
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author pete
 */
public class JsonViewFormatter implements ViewFormatter<ObjectNode> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ObjectMapper mapper;
    private final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider;
    private ObjectNode root;
    private ObjectId viewRoot;
    private Map<ObjectId, JsonNode> level;
    private Map<ObjectId, JsonNode> nextLevel;
    private static final String COUNT_PREFIX = "count_";
    private static final String ALL_PREFIX = "all_";
    private static final String LATEST_PREFIX = "latest_";

    public JsonViewFormatter(ObjectMapper mapper, WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider) {
        this.mapper = mapper;
        this.writtenEventProvider = writtenEventProvider;
        this.level = new HashMap<>();
        this.nextLevel = new HashMap<>();
    }

    @Override
    public void setRoot(ObjectId viewRoot) {
        this.viewRoot = viewRoot;
        root = mapper.createObjectNode();
        root.put(ReservedFields.VIEW_OBJECT_ID, viewRoot.toStringForm());
        level.put(viewRoot, root);
    }

    @Override
    public void nextLevel() {
        level.clear();
        level.putAll(nextLevel);
        nextLevel.clear();
    }

    @Override
    public void nextPath() {
        level.clear();
        level.put(viewRoot, root);
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
    public void addReferenceNode(ViewReference reference, List<ObjectId> presentDestinations) {
        ModelPathStepType stepType = reference.getStepType();

        JsonNode node = level.get(reference.getOriginId());
        if (node == null || !node.isObject()) {
            LOG.warn("Expected object node when adding reference with origin " + reference.getOriginId()
                + ". Found " + (node != null ? node.getNodeType() : null) + " in model path "
                + reference.getPath().getId());

            return;
        }

        ObjectNode object = (ObjectNode) node;
        addReferenceToObject(stepType, object, reference, presentDestinations);
    }

    @Override
    public void addValueNode(ViewValue value) {
        JsonNode node = level.get(value.getObjectId());
        if (node == null || !node.isObject()) {
            LOG.warn("Expected object node when adding value with id " + value.getObjectId()
                + ". Found " + (node != null ? node.getNodeType() : null) + " in model path "
                + value.getPath().getId());

            return;
        }

        ObjectNode object = (ObjectNode) node;
        for (Map.Entry<String, OpaqueFieldValue> entry : value.getResult().entrySet()) {
            JsonNode valueNode = writtenEventProvider.recoverFieldValue(entry.getValue());
            if (valueNode != null) {
                object.put(entry.getKey(), writtenEventProvider.recoverFieldValue(entry.getValue()));
            }
        }
    }

    private void addReferenceToObject(ModelPathStepType stepType, ObjectNode object, ViewReference reference,
        List<ObjectId> presentDestinations) {
        if (ModelPathStepType.count.equals(stepType)) {

            object.put(COUNT_PREFIX + reference.getRefFieldName(), presentDestinations.size());

        } else if (ModelPathStepType.latest_backRef.equals(stepType)) {
            if (presentDestinations.isEmpty()) {
                return;
            }
            ObjectId destination = presentDestinations.get(0);
            ObjectNode destinationNode = mapper.createObjectNode();
            destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

            object.put(LATEST_PREFIX + reference.getRefFieldName(), destinationNode);
            nextLevel.put(destination, destinationNode);

        } else if (ModelPathStepType.backRefs.equals(stepType)) {

            ArrayNode array = mapper.createArrayNode();
            for (ObjectId destination : presentDestinations) {
                ObjectNode destinationNode = mapper.createObjectNode();
                destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

                array.add(destinationNode);
                nextLevel.put(destination, destinationNode);
            }
            if (array.size() > 0) {
                object.put(ALL_PREFIX + reference.getRefFieldName(), array);
            }

        } else if (ModelPathStepType.ref.equals(stepType)) {
            if (presentDestinations.isEmpty()) {
                return;
            }
            ObjectId destination = presentDestinations.get(0);
            ObjectNode destinationNode = mapper.createObjectNode();
            destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

            object.put(reference.getRefFieldName(), destinationNode);
            nextLevel.put(destination, destinationNode);

        } else if (ModelPathStepType.refs.equals(stepType)) {

            ArrayNode array = mapper.createArrayNode();
            for (ObjectId destination : presentDestinations) {
                ObjectNode destinationNode = mapper.createObjectNode();
                destinationNode.put(ReservedFields.VIEW_OBJECT_ID, destination.toStringForm());

                array.add(destinationNode);
                nextLevel.put(destination, destinationNode);
            }
            if (array.size() > 0) {
                object.put(reference.getRefFieldName(), array);
            }

        }
    }
}

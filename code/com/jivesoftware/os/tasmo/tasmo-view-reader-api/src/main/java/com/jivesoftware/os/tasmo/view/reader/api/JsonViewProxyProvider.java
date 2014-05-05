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
package com.jivesoftware.os.tasmo.view.reader.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class JsonViewProxyProvider {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Nullable
    public <V> V getViewProxy(@Nullable ObjectNode objectNode, Class<V> modelClass) {
        if (objectNode == null) {
            return null;
        }
        return getObjectNodeAdapter(objectNode, objectNode, modelClass);
    }

    @SuppressWarnings("unchecked")
    private static <V> V getObjectNodeAdapter(ObjectNode root, ObjectNode objectNode, Class<V> clazz) {
        return (V) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz},
            new JsonViewProxyProvider.ViewProxyInvocationHandler(root, objectNode, clazz));
    }

    static class ViewProxyInvocationHandler implements InvocationHandler {

        private final ObjectNode root;
        private final ObjectNode objectNode;
        private final Class<?> topLevelInterface;

        public ViewProxyInvocationHandler(ObjectNode root, ObjectNode objectNode, Class<?> topLevelInterface) {
            this.root = (root == null) ? objectNode : root;
            this.objectNode = objectNode;
            this.topLevelInterface = topLevelInterface;
        }

        ObjectNode getObjectNode() {
            return objectNode;
        }

        Class<?> getTopLevelInterface() {
            return topLevelInterface;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String fieldName = method.getName();

            if (fieldName.equals("asMap")) {
                return objectMapper.treeToValue(objectNode, Map.class);
            } else if (fieldName.equals("toString")) {
                return "View proxy for interface " + topLevelInterface.getName()
                    + " with JSON " + objectMapper.writeValueAsString(root);
            }

            JsonNode jsonNode = objectNode.get(fieldName);
            Class<?> returnType = method.getReturnType();

            if (jsonNode == null) {
                if (method.getReturnType().isArray()) {
                    return Array.newInstance(method.getReturnType().getComponentType(), 0);
                } else if (method.isAnnotationPresent(Nullable.class)) {
                    return null;
                } else if (fieldName.startsWith("count_")) { //ReservedFields.COUNT_BACK_REF_FIELD_PREFIX
                    // When no events backref this object, count returned by materializer is missing - fake it
                    return new Integer(0);
                } else if (returnType.isInterface()) {
                    throw new IllegalStateException("Attempting to get data from interface type");
                } else {
                    throw new IllegalStateException("Attempting to get data that was not found in: " + topLevelInterface.getSimpleName());
                }
            }

            return convert(jsonNode, returnType);
        }

        private Object convert(JsonNode jsonNode, Class<?> type) {
            if (type.isInterface() && jsonNode.isObject()) {
                return getObjectNodeAdapter(root, (ObjectNode) jsonNode, type);
            } else if (type.isArray()) {
                ArrayNode arrayNode = (ArrayNode) jsonNode;
                Class<?> elementType = type.getComponentType();
                Object[] result = (Object[]) Array.newInstance(elementType, arrayNode.size());
                int i = 0;
                for (JsonNode node : arrayNode) {
                    result[i++] = convert(node, elementType);
                }
                return result;
            } else {
                try {
                    return objectMapper.treeToValue(jsonNode, type);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to convert " + jsonNode + " to type " + type.getName(), e);
                }
            }
        }
    }
}

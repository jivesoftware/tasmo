package com.jivesoftware.os.tasmo.view.reader.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Produces wrapper classes that expose JSON data as typed interfaces declared in model (events or views).
 * Currently implemented with dynamic {@link Proxy}.
 */
public class ModelAdapterFactory {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Nullable
    public static <V> V createModelAdapter(@Nullable ObjectNode objectNode, Class<V> modelClass) {
        if (objectNode == null) {
            return null;
        }
        return getObjectNodeAdapter(objectNode, objectNode, modelClass);
    }

    public static <V> Optional<V> createModelAdapter(Optional<ObjectNode> objectNode, Class<V> modelClass) {
        if (objectNode.isPresent()) {
            return Optional.of(getObjectNodeAdapter(objectNode.get(), objectNode.get(), modelClass));
        }
        return Optional.absent();
    }

    @SuppressWarnings("unchecked")
    private static <V> V getObjectNodeAdapter(ObjectNode root, ObjectNode objectNode, Class<V> clazz) {
        return (V) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[] { clazz, ModelAdapter.class },
            new ModelAdapterInvocationHandler(root, objectNode, clazz));
    }

    static String getFieldName(Method method) {
        String fieldName = method.getName();
        if (fieldName.equals(ModelingConstants.VIEW_BASE_ALIAS)) {
            fieldName = "objectId";
        } else if (method.isAnnotationPresent(BackRef.class)) {
            BackRef backRefAnnotation = method.getAnnotation(BackRef.class);
            fieldName = backRefAnnotation.type().getFieldNamePrefix() + backRefAnnotation.via();
        }
        return fieldName;
    }

    static class ModelAdapterInvocationHandler implements InvocationHandler {
        private final ObjectNode root;
        private final ObjectNode objectNode;
        private final Class<?> topLevelInterface;

        public ModelAdapterInvocationHandler(ObjectNode root, ObjectNode objectNode, Class<?> topLevelInterface) {
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
            String fieldName = getFieldName(method);
            if (fieldName.equals("asMap")) {
                return objectMapper.treeToValue(objectNode, Map.class);
            } else if (fieldName.equals("toString")) {
                return "Model Adapter for interface " + topLevelInterface.getName()
                    + " with JSON " + objectMapper.writeValueAsString(root);
            }
            JsonNode jsonNode = objectNode.get(fieldName);
            if (jsonNode == null) {
                BackRef backRefAnnotation;
                if (method.getReturnType().isArray()) {
                    return Array.newInstance(method.getReturnType().getComponentType(), 0);
                } else if (method.isAnnotationPresent(Nullable.class)) {
                    return null;
                } else if ((backRefAnnotation = method.getAnnotation(BackRef.class)) != null && backRefAnnotation.type() == BackRefType.COUNT) {
                    // When no events backref this object, count returned by materializer is missing - fake it
                    return new Integer(0);
                } else {
                    throw new IllegalArgumentException(
                        "Element expected but not present in JSON " + topLevelInterface.getName() + "." + fieldName + ": " + root);
                }
            }
            Class<?> returnType = method.getReturnType();
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

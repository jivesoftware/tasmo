/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.testng.Assert;

/**
 *
 */
class ViewExpectations {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ViewValueStore viewValueStore;
    private final Map<ViewKey, ModelPath> viewModelPaths = Maps.newHashMap();
    private final List<Expectation> expectations = Lists.newArrayList();
    private final ViewPathKeyProvider viewPathKeyProvider;

    ViewExpectations(ViewValueStore viewValueStore, ViewPathKeyProvider viewPathKeyProvider) {
        this.viewValueStore = viewValueStore;
        this.viewPathKeyProvider = viewPathKeyProvider;
    }

    public void init(Views views) {
        List<ViewBinding> bindings = views.getViewBindings();
        for (ViewBinding viewBinding : bindings) {
            for (ModelPath modelPath : viewBinding.getModelPaths()) {
                viewModelPaths.put(new ViewKey(viewBinding.getViewClassName(), modelPath.getId()), modelPath);
            }
        }
    }

    void addExpectation(ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value) {
        ObjectId viewId = new ObjectId(viewClassName, rootId.getId());
        ModelPath modelPath = viewModelPaths.get(new ViewKey(viewClassName, viewFieldName));
        long modelPathIdHashcode = viewPathKeyProvider.modelPathHashcode(modelPath.getId());
        expectations.add(new Expectation(viewId, viewClassName, modelPathIdHashcode, modelPath, pathIds, fieldName, value));
    }

    void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {

        for (Expectation expectation : expectations) {
            ViewValue got = viewValueStore.get(tenantIdAndCentricId,
                expectation.viewId,
                expectation.modelPathHashcode,
                expectation.modelPathInstanceIds);
            JsonNode node = null;
            if (got != null) {
                node = MAPPER.readValue(new ByteArrayInputStream(got.getValue()), JsonNode.class);
            }
            try {
                if (expectation.value == null) {
                    if (node != null) {
                        JsonNode value = node.get(expectation.fieldName);
                        if (value != null) {
                            Assert.assertTrue(value instanceof NullNode, "Wanted field:" + expectation.fieldName + " with value:" + value
                                + " to be an instance of NullNode but it wasn't. " + node + " "
                                + expectation.toString());
                        }
                    }
                } else {
                    JsonNode toTest;
                    if (node != null && expectation.fieldName != null) {
                        toTest = node.get(expectation.fieldName);
                    } else {
                        toTest = node;
                    }
                    JsonNode convertValue = MAPPER.convertValue(expectation.value, JsonNode.class);
                    Assert.assertEquals(toTest, convertValue,
                        expectation.toString() + " WAS:" + toTest + " WANTED:" + convertValue);
                }
            } catch (IllegalArgumentException x) {
                System.out.println("Failed while asserting " + expectation);
                throw x;
            }
        }
    }

    void clear() {
        expectations.clear();
    }

    static class ViewKey {

        String viewClassName;
        String viewFieldName;

        ViewKey(String viewClassName, String viewFieldName) {
            this.viewClassName = viewClassName;
            this.viewFieldName = viewFieldName;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 71 * hash + (this.viewClassName != null ? this.viewClassName.hashCode() : 0);
            hash = 71 * hash + (this.viewFieldName != null ? this.viewFieldName.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ViewKey other = (ViewKey) obj;
            if (this.viewClassName != other.viewClassName && (this.viewClassName == null || !this.viewClassName.equals(other.viewClassName))) {
                return false;
            }
            if (this.viewFieldName != other.viewFieldName && (this.viewFieldName == null || !this.viewFieldName.equals(other.viewFieldName))) {
                return false;
            }
            return true;
        }
    }

    static class Expectation {

        ObjectId viewId;
        String viewClassName;
        long modelPathHashcode;
        ModelPath path;
        ObjectId[] modelPathInstanceIds;
        String fieldName;
        Object value;

        Expectation(ObjectId viewId,
            String viewClassName,
            long modelPathHashcode,
            ModelPath path,
            ObjectId[] modelPathInstanceIds,
            String fieldName,
            Object value) {
            this.viewId = viewId;
            this.viewClassName = viewClassName;
            this.modelPathHashcode = modelPathHashcode;
            this.path = path;
            this.modelPathInstanceIds = modelPathInstanceIds;
            this.fieldName = fieldName;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Expectation{" + "viewId=" + viewId + ", viewClassName=" + viewClassName + ", modelPathId=" + modelPathHashcode + ", path=" + path
                + ", modelPathInstanceIds=" + Arrays.deepToString(modelPathInstanceIds) + ", fieldName=" + fieldName + ", value=" + value + '}';
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.viewId);
            hash = 29 * hash + Objects.hashCode(this.viewClassName);
            hash = 29 * hash + (int) (this.modelPathHashcode ^ (this.modelPathHashcode >>> 32));
            hash = 29 * hash + Objects.hashCode(this.path);
            hash = 29 * hash + Arrays.deepHashCode(this.modelPathInstanceIds);
            hash = 29 * hash + Objects.hashCode(this.fieldName);
            hash = 29 * hash + Objects.hashCode(this.value);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Expectation other = (Expectation) obj;
            if (!Objects.equals(this.viewId, other.viewId)) {
                return false;
            }
            if (!Objects.equals(this.viewClassName, other.viewClassName)) {
                return false;
            }
            if (this.modelPathHashcode != other.modelPathHashcode) {
                return false;
            }
            if (!Objects.equals(this.path, other.path)) {
                return false;
            }
            if (!Arrays.deepEquals(this.modelPathInstanceIds, other.modelPathInstanceIds)) {
                return false;
            }
            if (!Objects.equals(this.fieldName, other.fieldName)) {
                return false;
            }
            if (!Objects.equals(this.value, other.value)) {
                return false;
            }
            return true;
        }

    }
}

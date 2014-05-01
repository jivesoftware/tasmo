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
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;

/**
 *
 */
class Expectations {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ViewValueStore viewValueStore;
    private final Map<ViewKey, ModelPath> viewModelPaths = Maps.newHashMap();
    private final List<Expectation> expectations = Lists.newArrayList();

    public Expectations(ViewValueStore viewValueStore, Views views) {
        this.viewValueStore = viewValueStore;

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
        expectations.add(new Expectation(viewId, viewClassName, viewFieldName, modelPath, pathIds, fieldName, value));
    }

    void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {
        for (Expectation expectation : expectations) {
            ViewValue got = viewValueStore.get(tenantIdAndCentricId,
                expectation.viewId,
                expectation.modelPathId,
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
                            Assert.assertTrue(value instanceof NullNode, expectation.toString());
                        }
                    }
                } else {
                    JsonNode toTest;
                    if (node != null && expectation.fieldName != null) {
                        toTest = node.get(expectation.fieldName);
                    } else {
                        toTest = node;
                    }
                    Assert.assertEquals(toTest, MAPPER.convertValue(expectation.value, JsonNode.class), expectation.toString());
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

        public ViewKey(String viewClassName, String viewFieldName) {
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
        String modelPathId;
        ModelPath path;
        ObjectId[] modelPathInstanceIds;
        String fieldName;
        Object value;

        public Expectation(ObjectId viewId,
            String viewClassName,
            String viewFieldName,
            ModelPath path,
            ObjectId[] modelPathInstanceIds,
            String fieldName,
            Object value) {
            this.viewId = viewId;
            this.viewClassName = viewClassName;
            this.modelPathId = viewFieldName;
            this.path = path;
            this.modelPathInstanceIds = modelPathInstanceIds;
            this.fieldName = fieldName;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Expectation{" + "viewId=" + viewId + ", viewClassName=" + viewClassName + ", modelPathId=" + modelPathId + ", path=" + path
                + ", modelPathInstanceIds=" + Arrays.deepToString(modelPathInstanceIds) + ", fieldName=" + fieldName + ", value=" + value + '}';
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 37 * hash + (this.viewId != null ? this.viewId.hashCode() : 0);
            hash = 37 * hash + (this.viewClassName != null ? this.viewClassName.hashCode() : 0);
            hash = 37 * hash + (this.modelPathId != null ? this.modelPathId.hashCode() : 0);
            hash = 37 * hash + (this.path != null ? this.path.hashCode() : 0);
            hash = 37 * hash + (this.modelPathInstanceIds != null ? this.modelPathInstanceIds.hashCode() : 0);
            hash = 37 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
            hash = 37 * hash + (this.value != null ? this.value.hashCode() : 0);
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
            if (this.viewId != other.viewId && (this.viewId == null || !this.viewId.equals(other.viewId))) {
                return false;
            }
            if ((this.viewClassName == null) ? (other.viewClassName != null) : !this.viewClassName.equals(other.viewClassName)) {
                return false;
            }
            if ((this.modelPathId == null) ? (other.modelPathId != null) : !this.modelPathId.equals(other.modelPathId)) {
                return false;
            }
            if (this.path != other.path && (this.path == null || !this.path.equals(other.path))) {
                return false;
            }
            if (this.modelPathInstanceIds != other.modelPathInstanceIds
                && (this.modelPathInstanceIds == null || !this.modelPathInstanceIds.equals(other.modelPathInstanceIds))) {
                return false;
            }
            if ((this.fieldName == null) ? (other.fieldName != null) : !this.fieldName.equals(other.fieldName)) {
                return false;
            }
            if (this.value != other.value && (this.value == null || !this.value.equals(other.value))) {
                return false;
            }
            return true;
        }
    }
}

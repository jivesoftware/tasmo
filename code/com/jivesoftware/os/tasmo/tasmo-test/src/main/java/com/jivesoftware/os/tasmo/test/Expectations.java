/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;

/**
 *
 */
public class Expectations {

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

    public void buildExpectations(long testCase, Expectations expectations, ViewBinding binding, EventFire input, Set<Id> deletedIds) throws Exception {
        for (Branch viewBranch : calculateBranches(input, binding.getModelPaths().get(0), deletedIds)) {

            for (Map.Entry<String, String> entry : input.getLeafNodeFields().entrySet()) {
                ObjectId[] branchIds = viewBranch.getBranchIds();

                System.out.println("Adding view path expecation: " + binding.getModelPaths().get(0).getId()
                        + Arrays.toString(branchIds) + ":" + entry.getKey() + "->" + (viewBranch.isDeleted() ? null : entry.getValue()));

                expectations.addExpectation(testCase, branchIds[0], binding.getViewClassName(),
                        binding.getModelPaths().get(0).getId(), branchIds, entry.getKey(),
                        viewBranch.isDeleted() ? null : entry.getValue());
            }
        }

    }

    public static class Branch {

        private final ObjectId[] branchIds;
        private final boolean deleted;

        public Branch(ObjectId[] branchIds, boolean deleted) {
            this.branchIds = branchIds;
            this.deleted = deleted;
        }

        public ObjectId[] getBranchIds() {
            return branchIds;
        }

        public boolean isDeleted() {
            return deleted;
        }
    }

    private void walkTree(IdTreeNode parent, List<Id[]> allBranches) {
        if (parent.children().isEmpty()) {
            List<Id> ids = new ArrayList<>();
            while (true) {
                ids.add(parent.value());
                parent = parent.parent();
                if (parent == null) {
                    break;
                }
            }

            ids = Lists.reverse(ids);
            allBranches.add(ids.toArray(new Id[ids.size()]));
        } else {
            for (IdTreeNode child : parent.children()) {
                walkTree(child, allBranches);
            }
        }
    }

    public List<Branch> calculateBranches(EventFire input, ModelPath path, Set<Id> deletedIds) {
        List<Id[]> branchIds = new ArrayList<>();
        walkTree(input.getIdTree(), branchIds);
        List<Branch> branches = new ArrayList<>();

        for (Id[] branch : branchIds) {
            boolean delete = false;
            ObjectId[] objectIds = new ObjectId[branch.length];
            for (int i = 0; i < branch.length; i++) {
                ModelPathStep step = path.getPathMembers().get(i);
                String className = step.getStepType().isBackReferenceType() ? step.getDestinationClassNames().iterator().next()
                        : step.getOriginClassNames().iterator().next();
                objectIds[i] = new ObjectId(className, branch[i]);

                delete |= deletedIds.contains(branch[i]);
            }

            branches.add(new Branch(objectIds, delete));
        }

        return branches;

    }

    void addExpectation(long testCase, ObjectId rootId, String viewClassName, String viewFieldName, ObjectId[] pathIds, String fieldName, Object value) {
        ObjectId viewId = new ObjectId(viewClassName, rootId.getId());
        ModelPath modelPath = viewModelPaths.get(new ViewKey(viewClassName, viewFieldName));
        expectations.add(new Expectation(testCase, viewId, viewClassName, viewFieldName, modelPath, pathIds, fieldName, value));
    }

    void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws IOException {
        List<AssertNode> asserts = new ArrayList<>();
        for (Expectation expectation : expectations) {
            String got = viewValueStore.get(tenantIdAndCentricId,
                    expectation.viewId,
                    expectation.modelPathId,
                    expectation.modelPathInstanceIds);
            JsonNode node = null;
            if (got != null) {
                got = MAPPER.readValue(got, String.class);
                node = MAPPER.readValue(got, JsonNode.class);
            }
            try {
                if (expectation.value == null) {
                    if (node != null) {
                        JsonNode value = node.get(expectation.fieldName);
                        if (value != null) {
                            Assert.assertTrue(value instanceof NullNode,
                                    "value:" + value + " expected to be a NullNode but was " + value.getClass() + "  " + expectation.toString());
                        }
                    }
                } else {
                    JsonNode was;
                    if (node != null && expectation.fieldName != null) {
                        was = node.get(expectation.fieldName);
                    } else {
                        was = node;
                    }
                    JsonNode want = MAPPER.convertValue(expectation.value, JsonNode.class);
                    //Assert.assertEquals(was, want, expectation.toString() + " WANTED: " + want + " but WAS:" + was);

                    asserts.add(new AssertNode(expectation, want, was));
                }
            } catch (IllegalArgumentException x) {
                System.out.println("Failed while asserting " + expectation);
                throw x;
            }
        }
        StringBuilder errors = new StringBuilder();
        for (AssertNode a : asserts) {
            if (!a.isTrue()) {
                errors.append(a.expectation.toString()).append(" WANTED: ").append(a.want).append(" but WAS:").append(a.was).append("\n");
            }
        }
        if (errors.length() > 0) {
            Assert.fail(errors.toString());
        }
    }

    static class AssertNode {

        Expectation expectation;
        JsonNode want;
        JsonNode was;

        public AssertNode(Expectation expectation, JsonNode want, JsonNode was) {
            this.expectation = expectation;
            this.want = want;
            this.was = was;
        }

        public boolean isTrue() {
            return want.equals(was);
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

        long testCase;
        ObjectId viewId;
        String viewClassName;
        String modelPathId;
        ModelPath path;
        ObjectId[] modelPathInstanceIds;
        String fieldName;
        Object value;

        public Expectation(long testCase,
                ObjectId viewId,
                String viewClassName,
                String viewFieldName,
                ModelPath path,
                ObjectId[] modelPathInstanceIds,
                String fieldName,
                Object value) {
            this.testCase = testCase;
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
            return "Expectation{"
                    + "testCase=" + testCase
                    + ", viewId=" + viewId
                    + ", viewClassName=" + viewClassName
                    + ", modelPathId=" + modelPathId
                    + ", path=" + path
                    + ", modelPathInstanceIds=" + Arrays.deepToString(modelPathInstanceIds)
                    + ", fieldName=" + fieldName
                    + ", value=" + value
                    + '}';
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

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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import java.io.ByteArrayInputStream;
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

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
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

                LOG.trace("Adding view path expecation: " + binding.getModelPaths().get(0).getId()
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

    void assertExpectations(TenantIdAndCentricId tenantIdAndCentricId, ObjectNode view) throws IOException {
        List<AssertNode> asserts = new ArrayList<>();
        for (Expectation expectation : expectations) {
            ViewValue got = viewValueStore.get(tenantIdAndCentricId,
                    expectation.viewId,
                    expectation.modelPathId,
                    expectation.modelPathInstanceIds);
            JsonNode node = null;
            if (got != null) {
                node = MAPPER.readValue(new ByteArrayInputStream(got.getValue()), JsonNode.class);
            }
            assertExpectation(expectation, node, asserts);
            node = findExpectationNode(expectation.path.getPathMembers(), expectation.modelPathInstanceIds, view, expectation.value != null);
            if (node != null) {
                assertExpectation(expectation, node, asserts);
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

    private void assertExpectation(Expectation expectation, JsonNode node, List<AssertNode> asserts) {
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

    private JsonNode findExpectationNode(List<ModelPathStep> pathMembers, ObjectId[] ids, JsonNode node, boolean shouldBePresent) {
        if (shouldBePresent) {
            Assert.assertNotNull(node);
        } else if (node == null) {
            return null;
        }
        if (ids.length == 1) {
            String objectId = node.get(ReservedFields.VIEW_OBJECT_ID).asText();
            Assert.assertEquals(objectId, ids[0].toStringForm(), "Unexpected objectId");
            return node;
        }
        ModelPathStep firstStep = pathMembers.get(0);
        String fieldName = firstStep.getRefFieldName();
        switch (firstStep.getStepType()) {
            case backRefs:
                fieldName = ReservedFields.ALL_BACK_REF_FIELD_PREFIX + fieldName;
                break;
            case latest_backRef:
                fieldName = ReservedFields.LATEST_BACK_REF_FIELD_PREFIX + fieldName;
                break;
            case count:
                // Count nodes don't bring values, nothing to validate
                return null;
        }
        JsonNode subNode = node.get(fieldName);
        if (shouldBePresent) {
            Assert.assertNotNull(subNode, "Expected field " + fieldName + " in JSON " + node);
        } else if (subNode == null) {
            return null;
        }
        if (subNode instanceof ArrayNode) {
            Assert.assertTrue(firstStep.getStepType() == ModelPathStepType.backRefs || firstStep.getStepType() == ModelPathStepType.refs,
                    "Encountered array for " + fieldName + " in " + node + ", single value expected");
            JsonNode nodeInArray = null;
            for (JsonNode jsonNode : subNode) {
                JsonNode idNode = jsonNode.get(ReservedFields.VIEW_OBJECT_ID);
                if (idNode != null && ids[1].toStringForm().equals(idNode.asText())) {
                    nodeInArray = jsonNode;
                    break;
                }
            }
            // Technically we could skip this, assign jsonNode to subNode directly above and let recursion handle it.
            // But the assertion wouldn't be informative in this case.
            if (shouldBePresent) {
                Assert.assertNotNull(nodeInArray, "Didn't find expected id " + ids[1] + " in " + fieldName + " of " + node);
            } else if (nodeInArray == null) {
                return null;
            }
            subNode = nodeInArray;
        }
        return findExpectationNode(pathMembers.subList(1, pathMembers.size()), Arrays.copyOfRange(ids, 1, ids.length), subNode, shouldBePresent);
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

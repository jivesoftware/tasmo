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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;

/**
 *
 */
class Expectations {

    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;
    private final ExistenceStore existenceStore;
    private final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider;
    private final List<ValueExpectation> valueExpectations = Lists.newArrayList();
    private final List<ReferenceExpectation> refExpectations = Lists.newArrayList();
    private final List<ExistenceExpectation> existenceExpectations = Lists.newArrayList();
    private final ObjectMapper mapper = new ObjectMapper();

    public Expectations(WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider,
        EventValueStore eventValueStore, ReferenceStore referenceStore, ExistenceStore existenceStore) {
        this.writtenEventProvider = writtenEventProvider;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
        this.existenceStore = existenceStore;
    }

    void addValueExpectation(ObjectId valueObject, List<String> fields, List<Object> values) {
        if (fields.size() != values.size()) {
            throw new IllegalArgumentException("each field needs a value, each value needs a field");
        }

        valueExpectations.add(new ValueExpectation(valueObject, fields, values));
    }

    void addReferenceExpectation(ObjectId aId, String refField, List<ObjectId> bIds) {
        refExpectations.add(new ReferenceExpectation(aId, refField, bIds));
    }

    void addExistenceExpectation(ObjectId id, boolean exists) {
        existenceExpectations.add(new ExistenceExpectation(id, exists));
    }

    void assertExpectation(TenantIdAndCentricId tenantIdAndCentricId) throws Exception {
        assertValueExpectations(tenantIdAndCentricId);
        assertReferenceExpectations(tenantIdAndCentricId);
        assertExistenceExpectations(tenantIdAndCentricId);
    }

    private void assertValueExpectations(TenantIdAndCentricId tenantIdAndCentricId) {
        for (ValueExpectation valueExpectation : valueExpectations) {
            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got =
                eventValueStore.get(tenantIdAndCentricId, valueExpectation.valueObject, valueExpectation.fieldNamesAsArray());

            Assert.assertEquals(got.length, valueExpectation.fieldNames.size(), "Unexpected number of value results. "
                + valueExpectation.fieldNames.size() + " fields, and " + got.length + " results");

            Set<String> nonNullFields = new HashSet<>();

            for (ColumnValueAndTimestamp<String, OpaqueFieldValue, Long> result : got) {
                if (result != null) {
                    Object expectedValue = valueExpectation.valueForField(result.getColumn());
                    Assert.assertNotNull(expectedValue, result.getColumn() + " is not an expected field");
                    JsonNode resultJson = writtenEventProvider.recoverFieldValue(result.getValue());

                    JsonNode expectedJson;

                    if (expectedValue instanceof ObjectId) {
                        expectedJson = mapper.convertValue(((ObjectId) expectedValue).toStringForm(), JsonNode.class);
                    } else if (expectedValue instanceof Collection) {
                        Collection collection = (Collection) expectedValue;
                        if (!collection.isEmpty() && collection.iterator().next() instanceof ObjectId) {
                            List<String> stringForm = new ArrayList<>(collection.size());
                            for (Object element : collection) {
                                stringForm.add(((ObjectId) element).toStringForm());
                            }
                            expectedJson = mapper.convertValue(stringForm, JsonNode.class);
                        } else {
                            expectedJson = mapper.convertValue(expectedValue, JsonNode.class);
                        }
                    } else {
                        expectedJson = mapper.convertValue(expectedValue, JsonNode.class);
                    }

                    Assert.assertEquals(resultJson.asText(), expectedJson.asText());
                }
            }

            Assert.assertEquals(Sets.difference(valueExpectation.expectedNulls(), nonNullFields).size(), valueExpectation.expectedNulls().size());
        }
    }

    private void assertReferenceExpectations(TenantIdAndCentricId tenantIdAndCentricId) throws Exception {
        for (ReferenceExpectation referenceExpectation : refExpectations) {
            final List<ObjectId> bIds = new ArrayList<>();

            referenceStore.get_bIds(tenantIdAndCentricId, referenceExpectation.aId.getClassName(), referenceExpectation.refField,
                new Reference(referenceExpectation.aId, 0), new CallbackStream<Reference>() {
                @Override
                public Reference callback(Reference value) throws Exception {
                    if (value != null) {
                        bIds.add(value.getObjectId());
                    }
                    return value;
                }
            });

            Set<ObjectId> expected = new HashSet<>(referenceExpectation.bIds);
            Set<ObjectId> found = new HashSet<>(bIds);

            Assert.assertTrue(Sets.difference(expected, found).isEmpty());
            Assert.assertTrue(Sets.difference(found, expected).isEmpty());

            for (ObjectId bId : bIds) {
                final Set aIds = new HashSet<>();

                referenceStore.get_aIds(tenantIdAndCentricId, new Reference(bId, 0), Sets.newHashSet(referenceExpectation.aId.getClassName()),
                    referenceExpectation.refField, new CallbackStream<Reference>() {
                    @Override
                    public Reference callback(Reference value) throws Exception {
                        if (value != null) {
                            aIds.add(value.getObjectId());
                        }
                        return value;
                    }
                });

                Assert.assertTrue(aIds.contains(referenceExpectation.aId));
            }
        }
    }

    private void assertExistenceExpectations(TenantIdAndCentricId tenantIdAndCentricId) {
        for (ExistenceExpectation existenceExpectation : existenceExpectations) {
            Set<ObjectId> exists = existenceStore.getExistence(tenantIdAndCentricId.getTenantId(), Sets.newHashSet(existenceExpectation.objectId));
            if (existenceExpectation.exists) {
                Assert.assertTrue(exists.contains(existenceExpectation.objectId));
            } else {
                Assert.assertFalse(exists.contains(existenceExpectation.objectId));
            }
        }
    }

    void clear() {
        valueExpectations.clear();
        refExpectations.clear();
        existenceExpectations.clear();
    }

    private static class ValueExpectation {

        private final ObjectId valueObject;
        private final List<String> fieldNames;
        private final List<Object> values;

        public ValueExpectation(ObjectId valueObject, List<String> fieldNames, List<Object> values) {
            this.valueObject = valueObject;
            this.fieldNames = fieldNames;
            this.values = values;
        }

        Object valueForField(String field) {
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i).equals(field)) {
                    return values.get(i);
                }
            }

            return null;
        }

        String[] fieldNamesAsArray() {
            return fieldNames.toArray(new String[fieldNames.size()]);
        }

        Set<String> expectedNulls() {
            Set<String> nullFields = new HashSet<>();

            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) == null) {
                    nullFields.add(fieldNames.get(i));
                }
            }

            return nullFields;
        }
    }

    private static class ReferenceExpectation {

        private final ObjectId aId;
        private final String refField;
        private final List<ObjectId> bIds;

        public ReferenceExpectation(ObjectId aId, String refField, List<ObjectId> bIds) {
            this.aId = aId;
            this.refField = refField;
            this.bIds = bIds;
        }
    }

    private static class ExistenceExpectation {

        private final ObjectId objectId;
        private final boolean exists;

        public ExistenceExpectation(ObjectId objectId, boolean exists) {
            this.objectId = objectId;
            this.exists = exists;
        }
    }
}

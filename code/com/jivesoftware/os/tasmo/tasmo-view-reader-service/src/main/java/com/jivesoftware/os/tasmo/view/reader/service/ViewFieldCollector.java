/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ViewFieldCollector {

    private final Collector[] collectors;
    private final List<CollectedFieldValue> fieldValues = new ArrayList<>();

    public ViewFieldCollector(JsonViewMerger mapper, ModelPath path, String[] viewPathClasses) {
        List<Collector> list = Lists.newArrayList();
        List<ModelPathStep> pathMembers = path.getPathMembers();
        for (int i = 0; i < pathMembers.size(); i++) {
            ModelPathStep member = pathMembers.get(i);

            if (member.getStepType() == ModelPathStepType.value) {
                list.add(new ValueCollector(mapper, viewPathClasses[i]));
            } else if (member.getStepType() == ModelPathStepType.ref) {
                list.add(new RefCollector(mapper, viewPathClasses[i], member.getRefFieldName()));
            } else if (member.getStepType() == ModelPathStepType.backRefs) {
                list.add(new RefsCollector(mapper, viewPathClasses[i], FieldConstants.ALL_BACK_REF_FIELD_PREFIX + member.getRefFieldName()));
            } else if (member.getStepType() == ModelPathStepType.latest_backRef) {
                list.add(new LatestBackrefCollector(mapper, viewPathClasses[i], FieldConstants.LATEST_BACK_REF_FIELD_PREFIX + member.getRefFieldName()));
            } else {
                list.add(new RefsCollector(mapper, viewPathClasses[i], member.getRefFieldName()));
            }
        }

        this.collectors = list.toArray(new Collector[ list.size() ]);
    }

    public void collect(ObjectId[] fieldKey, String value, long timestamp) {
        fieldValues.add(new CollectedFieldValue(fieldKey, value, timestamp));
    }

    /**
     * Returns the populated field values, or null if nothing exists for this path
     *
     * @param canViewTheseIds
     * @return
     * @throws java.lang.Exception
     */
    public ObjectNode result(Set<Id> canViewTheseIds) throws Exception {

        for (CollectedFieldValue viewFieldValue : fieldValues) {
            ObjectId[] fieldKey = viewFieldValue.getFieldKey();
            String value = viewFieldValue.getValue();
            long timestamp = viewFieldValue.getTimestamp();

            int ci = 0;
            for (int i = 0; i < fieldKey.length; i++) {
                Id id = fieldKey[i].getId();
                if (canViewTheseIds.contains(id)) {
                    collectors[ci].process(collectors, ci, fieldKey[i].getId(), value, timestamp);
                } else if (i == 0) {
                    throw new UnauthorizedException();
                }
                ci++;
            }
        }
        return collectors[0].active();
    }

    private static class CollectedFieldValue {

        private final ObjectId[] fieldKey;
        private final String value;
        private final long timestamp;

        public CollectedFieldValue(ObjectId[] fieldKey, String value, long timestamp) {
            this.fieldKey = fieldKey;
            this.value = value;
            this.timestamp = timestamp;
        }

        public ObjectId[] getFieldKey() {
            return fieldKey;
        }

        public String getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "CollectedFieldValue{"
                    + "fieldKey=" + Arrays.toString(fieldKey)
                    + ", value=" + value
                    + ", timestamp=" + timestamp
                    + '}';
        }
    }
}

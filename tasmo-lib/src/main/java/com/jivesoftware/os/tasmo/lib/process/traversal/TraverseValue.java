/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TraverseValue implements StepTraverser {

    private final Set<String> fieldNames;
    private final int processingPathIndex;
    private final int pathIndex;
    private final boolean centric;

    public TraverseValue(Set<String> fieldNames,
            int processingPathIndex,
            int pathIndex,
            boolean centric) {

        this.fieldNames = fieldNames;
        this.processingPathIndex = processingPathIndex;
        this.pathIndex = pathIndex;
        this.centric = centric;
    }

    @Override
    public void process(TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext pathTraversalContext,
            PathContext pathContext,
            LeafContext leafContext,
            PathId from,
            StepStream streamTo) throws Exception {

        pathContext.setPathId(writtenEventContext, pathIndex, from.getObjectId(), from.getTimestamp());
        if (pathTraversalContext.isRemovalContext()) {
            List<ReferenceWithTimestamp> versions = leafContext.removeLeafNodeFields(writtenEventContext, pathContext);
            pathContext.addVersions(pathIndex, versions);
        } else {

            TenantIdAndCentricId tenantIdAndCentricId = (centric ? userCentricId : globalCentricId);

            String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
            ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>[] got = writtenEventContext
                    .getFieldValueReader()
                    .readFieldValues(tenantIdAndCentricId, from.getObjectId(), fieldNamesArray);

            final Map<String, ColumnValueAndTimestamp<String, OpaqueFieldValue, Long>> fieldValues = new HashMap<>();
            for (int i = 0; i < fieldNamesArray.length; i++) {
                fieldValues.put(fieldNamesArray[i], got[i]);
            }

            List<ReferenceWithTimestamp> versions = leafContext.populateLeafNodeFields(tenantIdAndCentricId,
                    writtenEventContext,
                    pathContext,
                    from.getObjectId(),
                    fieldNames,
                    fieldValues);
            pathContext.addVersions(pathIndex, versions);
        }
        PathId to = pathContext.getPathId(processingPathIndex);
        streamTo.stream(globalCentricId, userCentricId, writtenEventContext, pathTraversalContext, pathContext, leafContext, to);
    }

    @Override
    public String toString() {
        return "Value(fieldNames=" + fieldNames + ", processingPathIndex=" + processingPathIndex + ", pathIndex=" + pathIndex + ')';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.fieldNames);
        hash = 89 * hash + this.processingPathIndex;
        hash = 89 * hash + this.pathIndex;
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
        final TraverseValue other = (TraverseValue) obj;
        if (!Objects.equals(this.fieldNames, other.fieldNames)) {
            return false;
        }
        if (this.processingPathIndex != other.processingPathIndex) {
            return false;
        }
        if (this.pathIndex != other.pathIndex) {
            return false;
        }
        return true;
    }

}

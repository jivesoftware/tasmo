/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.List;
import java.util.Objects;

public class TraverseValue implements StepTraverser {

    private final List<String> fieldNames;
    private final int processingPathIndex;
    private final int pathIndex;

    public TraverseValue(List<String> fieldNames,
            int processingPathIndex,
            int pathIndex) {

        this.fieldNames = fieldNames;
        this.processingPathIndex = processingPathIndex;
        this.pathIndex = pathIndex;
    }

    @Override
    public void process(TenantIdAndCentricId tenantIdAndCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext pathTraversalContext,
            PathContext pathContext,
            LeafContext leafContext,
            PathId from,
            StepStream streamTo) throws Exception {

        pathContext.setPathId(writtenEventContext, pathIndex, from.getObjectId(), from.getTimestamp());
        if (pathTraversalContext.isRemovalContext()) {
            List<ReferenceWithTimestamp> versions = leafContext.removeLeafNodeFields(pathContext);
            pathContext.addVersions(pathIndex, versions);
        } else {
            List<ReferenceWithTimestamp> versions = leafContext.populateLeafNodeFields(tenantIdAndCentricId, pathContext, from.getObjectId(), fieldNames);
            pathContext.addVersions(pathIndex, versions);
        }
        PathId to = pathContext.getPathId(processingPathIndex);
        streamTo.stream(to);
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

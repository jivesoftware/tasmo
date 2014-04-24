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
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.List;

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
            PathTraversalContext context,
            PathId from,
            StepStream streamTo) throws Exception {

        context.setPathId(pathIndex, from.getObjectId(), from.getTimestamp());
        List<ReferenceWithTimestamp> versions = context.populateLeafNodeFields(tenantIdAndCentricId, from.getObjectId(), fieldNames);
        context.addVersions(pathIndex, versions);
        PathId to = context.getPathId(processingPathIndex);
        streamTo.stream(to);
    }

    @Override
    public String toString() {
        return "Value(fieldNames=" + fieldNames + ", processingPathIndex=" + processingPathIndex + ", pathIndex=" + pathIndex + ')';
    }

}

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
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;

/**
 *
 */
public class TraverseViewValueWriter implements StepTraverser {

    private final String viewClassName;
    private final String modelPathId;

    public TraverseViewValueWriter(String viewClassName, String modelPathId) {
        this.viewClassName = viewClassName;
        this.modelPathId = modelPathId;
    }

    @Override
    public void process(final TenantIdAndCentricId tenantIdAndCentricId,
            PathTraversalContext context,
            ReferenceWithTimestamp objectInstanceId,
            StepStream streamTo) throws Exception {
        context.writeViewFields(viewClassName, modelPathId, objectInstanceId.getObjectId());
    }

    @Override
    public String toString() {
        return "ViewValueWriterStep{" + "viewClassName=" + viewClassName + ", modelPathId=" + modelPathId + '}';
    }
}

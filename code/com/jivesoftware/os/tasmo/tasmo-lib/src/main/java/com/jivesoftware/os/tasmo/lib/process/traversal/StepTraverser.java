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
public interface StepTraverser {

    void process(TenantIdAndCentricId tenantIdAndCentricId,
        PathTraversalContext viewFieldContext,
        ReferenceWithTimestamp referenceWithTimestamp,
        StepStream streamTo) throws Exception;
}

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
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;

/**
 *
 */
public interface StepTraverser {

    void process(TenantIdAndCentricId globalCentricId,
            TenantIdAndCentricId userCentricId,
            WrittenEventContext writtenEventContext,
            PathTraversalContext viewFieldContext,
            PathContext pathContext,
            LeafContext leafContext,
            PathId pathId,
            StepStream streamTo) throws Exception;
}

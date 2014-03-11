/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.Reference;

/**
 *
 */
public interface ProcessStep {

    void process(TenantIdAndCentricId tenantIdAndCentricId,
        ViewFieldContext context,
        Reference objectIntanceId,
        StepStream streamTo) throws Exception;
}

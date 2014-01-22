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
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.reference.lib.Reference;

/**
 *
 */
public interface ProcessStep {

    void process(TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent,
        ViewFieldContext context,
        Reference objectIntanceId,
        StepStream streamTo) throws Exception;
}

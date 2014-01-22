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
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.List;

/**
 *
 */
public class ValueStep implements ProcessStep {

    private final EventValueStore eventValueStore;
    private final List<String> fieldNames;
    private final int processingPathIndex;
    private final int pathIndex;

    public ValueStep(EventValueStore eventValueStore,
        List<String> fieldNames,
        int processingPathIndex,
        int pathIndex) {
        this.eventValueStore = eventValueStore;
        this.fieldNames = fieldNames;
        this.processingPathIndex = processingPathIndex;
        this.pathIndex = pathIndex;
    }

    @Override
    public void process(TenantIdAndCentricId tenantIdAndCentricId,
        WrittenEvent writtenEvent,
        ViewFieldContext context,
        Reference objectInstanceId,
        StepStream streamTo) throws Exception {

        context.setPathId(pathIndex, objectInstanceId);
        context.populateLeadNodeFields(eventValueStore, writtenEvent, objectInstanceId.getObjectId(), fieldNames);

        streamTo.stream(context.getPathId(processingPathIndex));
    }
}

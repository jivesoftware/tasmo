/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.Set;

/**
 *
 */
class LeafwardStep implements ProcessStep {

    private final RefStreamer refStreamer;
    private final int pathIndex;
    private final Set<String> validDownStreamTypes;

    LeafwardStep(RefStreamer streamer, int pathIndex, Set<String> validDownStreamTypes) {
        this.refStreamer = streamer;
        this.pathIndex = pathIndex;
        this.validDownStreamTypes = validDownStreamTypes;
    }

    @Override
    public void process(final TenantIdAndCentricId tenantIdAndCentricId,
        final ViewFieldContext context,
        Reference objectInstanceId,
        final StepStream streamTo) throws Exception {
        context.setPathId(pathIndex, objectInstanceId);
        refStreamer.stream(tenantIdAndCentricId, objectInstanceId, new CallbackStream<Reference>() {
            @Override
            public Reference callback(Reference value) throws Exception {
                if (value != null && isValidDownStreamObject(value)) {
                    streamTo.stream(value);
                }
                return value;
            }
        });
    }

    private boolean isValidDownStreamObject(Reference toStream) {
        return validDownStreamTypes == null || validDownStreamTypes.isEmpty() || validDownStreamTypes.contains(toStream.getObjectId().getClassName());
    }
}

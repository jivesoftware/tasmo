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
class RootwardStep implements ProcessStep {

    private final RefStreamer refStreamer;
    private final int pathIndex;
    private final Set<String> validUpstreamTypes;

    RootwardStep(RefStreamer streamer, int pathIndex, Set<String> validUpstreamTypes) {
        this.refStreamer = streamer;
        this.pathIndex = pathIndex;
        this.validUpstreamTypes = validUpstreamTypes;
    }

    @Override
    public void process(final TenantIdAndCentricId tenantIdAndCentricId,
        final ViewFieldContext context,
        Reference objectIntanceId,
        final StepStream streamTo) throws Exception {
        context.setPathId(pathIndex, objectIntanceId);
        refStreamer.stream(tenantIdAndCentricId, objectIntanceId, new CallbackStream<Reference>() {
            @Override
            public Reference callback(Reference value) throws Exception {
                if (value != null && isValidUpStreamObject(value)) {
                    context.setPathId(pathIndex, value);
                    streamTo.stream(value);
                }
                return value;
            }
        });
    }

    private boolean isValidUpStreamObject(Reference toStream) {
        return validUpstreamTypes == null || validUpstreamTypes.isEmpty() || validUpstreamTypes.contains(toStream.getObjectId().getClassName());
    }
}

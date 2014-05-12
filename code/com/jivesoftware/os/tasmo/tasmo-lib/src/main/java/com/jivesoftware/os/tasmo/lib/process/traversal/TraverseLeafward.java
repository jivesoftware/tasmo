/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
class TraverseLeafward implements StepTraverser {

    private final RefStreamer streamer;
    private final int pathIndex;
    private final Set<String> validDownStreamTypes;

    TraverseLeafward(RefStreamer streamer, int pathIndex, Set<String> validDownStreamTypes) {
        this.streamer = streamer;
        this.pathIndex = pathIndex;
        this.validDownStreamTypes = validDownStreamTypes;
    }

    @Override
    public void process(final TenantIdAndCentricId tenantIdAndCentricId,
            final WrittenEventContext writtenEventContext,
            final PathTraversalContext context,
            final PathContext pathContext,
            final LeafContext leafContext,
            final PathId from,
            final StepStream streamTo) throws Exception {

        pathContext.setPathId(writtenEventContext, pathIndex, from.getObjectId(), from.getTimestamp());
        streamer.stream(tenantIdAndCentricId, from.getObjectId(), context.getThreadTimestamp(),
                new CallbackStream<ReferenceWithTimestamp>() {
                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                        if (to != null && isValidDownStreamObject(to)) {

                            ReferenceWithTimestamp ref = new ReferenceWithTimestamp(
                                    (streamer.isBackRefStreamer()) ? to.getObjectId() : from.getObjectId(),
                                    to.getFieldName(),
                                    to.getTimestamp());

                            pathContext.addVersions(pathIndex, Arrays.asList(ref));
                            streamTo.stream(new PathId(to.getObjectId(), to.getTimestamp()));
                        }
                        return to;
                    }
                });
    }

    private boolean isValidDownStreamObject(ReferenceWithTimestamp ref) {
        return validDownStreamTypes == null || validDownStreamTypes.isEmpty() || validDownStreamTypes.contains(ref.getObjectId().getClassName());
    }

    @Override
    public String toString() {
        return "Leafward(" + "streamer=" + streamer + ", pathIndex=" + pathIndex + ")";
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + Objects.hashCode(this.streamer);
        hash = 37 * hash + this.pathIndex;
        hash = 37 * hash + Objects.hashCode(this.validDownStreamTypes);
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
        final TraverseLeafward other = (TraverseLeafward) obj;
        if (!Objects.equals(this.streamer, other.streamer)) {
            return false;
        }
        if (this.pathIndex != other.pathIndex) {
            return false;
        }
        if (!Objects.equals(this.validDownStreamTypes, other.validDownStreamTypes)) {
            return false;
        }
        return true;
    }
}

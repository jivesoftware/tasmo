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
import com.jivesoftware.os.rcvs.api.CallbackStream;
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
class TraverseRootward implements StepTraverser {

    private final RefStreamer streamer;
    private final int pathIndex;
    private final Set<String> validUpstreamTypes;
    private final boolean centric;

    TraverseRootward(RefStreamer streamer, int pathIndex, Set<String> validUpstreamTypes, boolean centric) {
        this.streamer = streamer;
        this.pathIndex = pathIndex;
        this.validUpstreamTypes = validUpstreamTypes;
        this.centric = centric;
    }

    @Override
    public void process(final TenantIdAndCentricId globalCentricId,
            final TenantIdAndCentricId userCentricId,
            final WrittenEventContext writtenEventContext,
            final PathTraversalContext context,
            PathContext pathContext,
            final LeafContext leafContext,
            final PathId from,
            final StepStream streamTo) throws Exception {

        final PathContext copyOfPathContext = pathContext.getCopy();
        copyOfPathContext.setPathId(writtenEventContext, pathIndex, from.getObjectId(), from.getTimestamp());

        final TenantIdAndCentricId tenantIdAndCentricId = (centric ? userCentricId : globalCentricId);
        streamer.stream(writtenEventContext.getReferenceTraverser(),
                tenantIdAndCentricId,
                from.getObjectId(),
                context.getThreadTimestamp(),
                new CallbackStream<ReferenceWithTimestamp>() {
                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                        if (to != null && isValidUpStreamObject(to)) {

                            copyOfPathContext.setPathId(writtenEventContext, pathIndex, to.getObjectId(), to.getTimestamp());

                            ReferenceWithTimestamp ref = new ReferenceWithTimestamp(tenantIdAndCentricId,
                                    (streamer.isBackRefStreamer()) ? to.getObjectId() : from.getObjectId(),
                                    to.getFieldName(),
                                    to.getTimestamp());
                            copyOfPathContext.addVersions(pathIndex, Arrays.asList(ref));
                            streamTo.stream(globalCentricId, userCentricId, writtenEventContext, context, copyOfPathContext, leafContext,
                                    new PathId(to.getObjectId(), to.getTimestamp()));
                        }
                        return to;
                    }
                });
    }

    private boolean isValidUpStreamObject(ReferenceWithTimestamp ref) {
        return validUpstreamTypes == null || validUpstreamTypes.isEmpty() || validUpstreamTypes.contains(ref.getObjectId().getClassName());
    }

    @Override
    public String toString() {
        return "Rootward(" + "streamer=" + streamer + ", pathIndex=" + pathIndex + ')';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 61 * hash + Objects.hashCode(this.streamer);
        hash = 61 * hash + this.pathIndex;
        hash = 61 * hash + Objects.hashCode(this.validUpstreamTypes);
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
        final TraverseRootward other = (TraverseRootward) obj;
        if (!Objects.equals(this.streamer, other.streamer)) {
            return false;
        }
        if (this.pathIndex != other.pathIndex) {
            return false;
        }
        if (!Objects.equals(this.validUpstreamTypes, other.validUpstreamTypes)) {
            return false;
        }
        return true;
    }

}

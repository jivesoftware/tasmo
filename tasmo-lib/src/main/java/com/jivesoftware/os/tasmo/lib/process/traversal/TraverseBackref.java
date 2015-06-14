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
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.reference.lib.BackRefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class TraverseBackref implements StepTraverser {

    private final ModelPathStep initialModelPathMember;
    private final Set<String> validDownStreamTypes;
    private final boolean centric;

    public TraverseBackref(ModelPathStep initialModelPathMember,
            Set<String> validDownStreamTypes, boolean centric) {
        this.initialModelPathMember = initialModelPathMember;
        this.validDownStreamTypes = validDownStreamTypes;
        this.centric = centric;
    }

    @Override
    public void process(final TenantIdAndCentricId globalCentricId,
            final TenantIdAndCentricId userCentricId,
            final WrittenEventContext writtenEventContext,
            final PathTraversalContext context,
            final PathContext pathContext,
            final LeafContext leafContext,
            final PathId from,
            final StepStream streamTo) throws Exception {

        final RefStreamer streamer = new BackRefStreamer(initialModelPathMember.getOriginClassNames(),
                initialModelPathMember.getRefFieldName());

        streamer.stream(writtenEventContext.getReferenceTraverser(),
                (centric ? userCentricId : globalCentricId),
                from.getObjectId(),
                context.getThreadTimestamp(),
                new CallbackStream<ReferenceWithTimestamp>() {
                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                        if (to != null && isValidDownStreamObject(to)) {
                            streamTo.stream(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext,
                                    new PathId(to.getObjectId(), to.getTimestamp()));
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
        return "TraverseBackref." + initialModelPathMember;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + Objects.hashCode(this.initialModelPathMember);
        hash = 71 * hash + Objects.hashCode(this.validDownStreamTypes);
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
        final TraverseBackref other = (TraverseBackref) obj;
        if (!Objects.equals(this.initialModelPathMember, other.initialModelPathMember)) {
            return false;
        }
        if (!Objects.equals(this.validDownStreamTypes, other.validDownStreamTypes)) {
            return false;
        }
        return true;
    }

}

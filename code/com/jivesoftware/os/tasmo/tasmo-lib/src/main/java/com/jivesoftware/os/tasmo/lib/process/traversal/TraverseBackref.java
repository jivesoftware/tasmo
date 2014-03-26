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
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.reference.lib.BackRefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.RefStreamer;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class TraverseBackref implements StepTraverser {

    private final ModelPathStep initialModelPathMember;
    private final ReferenceStore referenceStore;
    private final Set<String> validDownStreamTypes;

    public TraverseBackref(ModelPathStep initialModelPathMember, ReferenceStore referenceStore,
            Set<String> validDownStreamTypes) {
        this.initialModelPathMember = initialModelPathMember;
        this.referenceStore = referenceStore;
        this.validDownStreamTypes = validDownStreamTypes;
    }

    @Override
    public void process(TenantIdAndCentricId tenantIdAndCentricId,
            final PathTraversalContext context,
            final PathId from,
            final StepStream streamTo) throws Exception {

        final RefStreamer streamer = new BackRefStreamer(referenceStore,
                initialModelPathMember.getOriginClassNames(),
                initialModelPathMember.getRefFieldName());

        streamer.stream(tenantIdAndCentricId, from.getObjectId(),
                new CallbackStream<ReferenceWithTimestamp>() {
                    @Override
                    public ReferenceWithTimestamp callback(ReferenceWithTimestamp to) throws Exception {
                        if (to != null && isValidDownStreamObject(to)) {
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
        return "TraverseBackref." + initialModelPathMember;
    }

}

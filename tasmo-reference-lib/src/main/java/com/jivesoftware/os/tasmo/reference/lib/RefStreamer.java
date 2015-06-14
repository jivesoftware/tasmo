package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;

/**
 *
 */
public interface RefStreamer {

    void stream(ReferenceTraverser referenceTraverser,
            TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            long readTime,
            CallbackStream<ReferenceWithTimestamp> referencedIdsStream) throws Exception;

    boolean isBackRefStreamer();
}

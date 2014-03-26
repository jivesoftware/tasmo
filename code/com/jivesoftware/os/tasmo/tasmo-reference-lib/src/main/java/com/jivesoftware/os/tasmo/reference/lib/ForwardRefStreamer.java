/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.Collections;

public class ForwardRefStreamer extends BaseRefStreamer {

    public ForwardRefStreamer(ReferenceStore referenceStore, String referringFieldName) {
        super(referenceStore, Collections.<String>emptySet(), referringFieldName);
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            final CallbackStream<ReferenceWithTimestamp> tos) throws Exception {

        referenceStore.streamForwardRefs(tenantIdAndCentricId,
                referringObjectId.getClassName(),
                referringFieldName,
                referringObjectId,
                tos);
    }

    @Override
    public boolean isBackRefStreamer() {
        return false;
    }

    @Override
    public String toString() {
        return "ForwardRefStreamer{" + "referringClassNames=" + referringClassNames + ", referringFieldName=" + referringFieldName + '}';
    }
}

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
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import java.util.Collections;

/**
 *
 */
public class B_IdsStreamer extends BaseRefStreamer {

    public B_IdsStreamer(ReferenceStore referenceStore, String referringFieldName) {
        super(referenceStore, Collections.<String>emptySet(), referringFieldName);
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId, Reference aId, final CallbackStream<Reference> bdIdsStream) throws Exception {

        referenceStore.get_bIds(tenantIdAndCentricId, aId.getObjectId().getClassName(), referringFieldName, aId, bdIdsStream);
    }
}

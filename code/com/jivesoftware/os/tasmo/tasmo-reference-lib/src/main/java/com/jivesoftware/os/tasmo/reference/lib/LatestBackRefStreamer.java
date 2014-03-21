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
import java.util.Set;

/**
 *
 * @author pete
 */
public class LatestBackRefStreamer extends BaseRefStreamer {

    public LatestBackRefStreamer(ReferenceStore referenceStore, Set<String> referringClassNames, String referringFieldName) {
        super(referenceStore, referringClassNames, referringFieldName);
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            final CallbackStream<ReferenceWithTimestamp> froms) throws Exception {

        referenceStore.streamLatestBackRef(tenantIdAndCentricId,
                referringObjectId,
                referringClassNames,
                referringFieldName,
                froms);
    }

    @Override
    public boolean isBackRefStreamer() {
        return true;
    }
}

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
import java.util.Set;

/**
 *
 * @author pete
 */
public class Latest_A_IdStreamer extends BaseRefStreamer {

    public Latest_A_IdStreamer(ReferenceStore referenceStore, Set<String> referringClassNames, String referringFieldName) {
        super(referenceStore, referringClassNames, referringFieldName);
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId, Reference bId, final CallbackStream<Reference> aIdsStream) throws Exception {
        referenceStore.get_latest_aId(tenantIdAndCentricId, bId, referringClassNames, referringFieldName, aIdsStream);
    }
}

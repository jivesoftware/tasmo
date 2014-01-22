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
 */
public class A_IdsStreamer extends BaseRefStreamer {

    public A_IdsStreamer(ReferenceStore referenceStore, Set<String> referringClassNames, String referringFieldName) {
        super(referenceStore, referringClassNames, referringFieldName);
    }

    @Override
    public void stream(TenantIdAndCentricId tenantIdAndCentricId, Reference bId, final CallbackStream<Reference> aIdsStream) throws Exception {

        //TODO create a ref store operation that handles this in one call to hbase
        CallbackStream<Reference> multiCallFilter = new CallbackStream<Reference>() {
            @Override
            public Reference callback(Reference v) throws Exception {
                if (v != null) {
                    aIdsStream.callback(v);
                }

                return v;
            }
        };

        referenceStore.get_aIds(tenantIdAndCentricId, bId, referringClassNames, referringFieldName, multiCallFilter);
    }
}

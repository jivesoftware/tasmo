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

/**
 *
 */
public interface RefStreamer {

    void stream(TenantIdAndCentricId tenantIdAndCentricId, Reference referringObjectId, CallbackStream<Reference> referencedIdsStream) throws Exception;
}

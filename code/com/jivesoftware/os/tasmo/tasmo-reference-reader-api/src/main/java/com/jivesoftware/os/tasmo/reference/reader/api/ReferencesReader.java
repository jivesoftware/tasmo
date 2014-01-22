/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.reader.api;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import java.util.List;

/**
 * Provides methods for reading references
 */
public interface ReferencesReader<V, E extends Throwable> {

    List<V> getReferencedBy(TenantId tenantId,
            Id userId,
            ObjectId pointOfReference,
            String nameOfReferencingEventClass,
            String nameOfReferencingEventField,
            ObjectId nextBatchStart,
            int resultCount) throws E;

}

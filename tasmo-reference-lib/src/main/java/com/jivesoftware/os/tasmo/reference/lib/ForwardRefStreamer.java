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
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.Collections;
import java.util.Objects;

public class ForwardRefStreamer implements RefStreamer {

    private final String referringFieldName;

    public ForwardRefStreamer(String referringFieldName) {
        this.referringFieldName = referringFieldName;
    }

    @Override
    public void stream(ReferenceTraverser referenceTraverser,
            TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            long readTime,
            final CallbackStream<ReferenceWithTimestamp> tos) throws Exception {

        referenceTraverser.traverseForwardRef(tenantIdAndCentricId,
                Collections.singleton(referringObjectId.getClassName()),
                referringFieldName,
                referringObjectId,
                readTime,
                tos);
    }

    @Override
    public boolean isBackRefStreamer() {
        return false;
    }

    @Override
    public String toString() {
        return "ForwardRefStreamer{ referringFieldName=" + referringFieldName + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.referringFieldName);
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
        final ForwardRefStreamer other = (ForwardRefStreamer) obj;
        if (!Objects.equals(this.referringFieldName, other.referringFieldName)) {
            return false;
        }
        return true;
    }

}

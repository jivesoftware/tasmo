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
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class BackRefStreamer implements RefStreamer {

    private final Set<String> referringClassNames;
    private final String referringFieldName;

    public BackRefStreamer(Set<String> referringClassNames,
            String referringFieldName) {
        this.referringClassNames = referringClassNames;
        this.referringFieldName = referringFieldName;
    }

    @Override
    public void stream(ReferenceTraverser referenceTraverser,
            TenantIdAndCentricId tenantIdAndCentricId,
            ObjectId referringObjectId,
            long readTime,
            final CallbackStream<ReferenceWithTimestamp> froms) throws Exception {

        referenceTraverser.traversBackRefs(tenantIdAndCentricId,
                referringClassNames,
                referringFieldName,
                referringObjectId,
                readTime,
                froms);
    }

    @Override
    public boolean isBackRefStreamer() {
        return true;
    }

    @Override
    public String toString() {
        return "BackRefStreamer{" + "referringClassNames=" + referringClassNames + ", referringFieldName=" + referringFieldName + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + Objects.hashCode(this.referringClassNames);
        hash = 71 * hash + Objects.hashCode(this.referringFieldName);
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
        final BackRefStreamer other = (BackRefStreamer) obj;
        if (!Objects.equals(this.referringClassNames, other.referringClassNames)) {
            return false;
        }
        if (!Objects.equals(this.referringFieldName, other.referringFieldName)) {
            return false;
        }
        return true;
    }
}

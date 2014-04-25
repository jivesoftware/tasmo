/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import java.util.Objects;
import java.util.Set;

public abstract class BaseRefStreamer implements RefStreamer {

    protected final ReferenceStore referenceStore; // factor out
    protected final Set<String> referringClassNames;
    protected final String referringFieldName;


    protected BaseRefStreamer(ReferenceStore referenceStore,
            Set<String> referringClassNames,
            String referringFieldName) {
        this.referenceStore = referenceStore;
        this.referringClassNames = referringClassNames;
        this.referringFieldName = referringFieldName;
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
        final BaseRefStreamer other = (BaseRefStreamer) obj;
        if (!Objects.equals(this.referringClassNames, other.referringClassNames)) {
            return false;
        }
        if (!Objects.equals(this.referringFieldName, other.referringFieldName)) {
            return false;
        }
        return true;
    }


}

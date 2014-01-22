/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import java.util.Set;

public abstract class BaseRefStreamer implements RefStreamer {

    protected final ReferenceStore referenceStore;
    protected final Set<String> referringClassNames;
    protected final String referringFieldName;


    protected BaseRefStreamer(ReferenceStore referenceStore, Set<String> referringClassNames, String referringFieldName) {
        this.referenceStore = referenceStore;
        this.referringClassNames = referringClassNames;
        this.referringFieldName = referringFieldName;
    }
}

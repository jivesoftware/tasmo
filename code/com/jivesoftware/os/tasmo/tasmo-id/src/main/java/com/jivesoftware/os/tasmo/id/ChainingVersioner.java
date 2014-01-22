/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.id;

public class ChainingVersioner {

    public ChainedVersion nextVersion(ChainedVersion version) {
        return new ChainedVersion(version.getVersion(), Long.toString(System.currentTimeMillis()));
    }
}

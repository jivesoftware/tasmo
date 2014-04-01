/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;

/**
 * @author jonathan.colt
 */
public class PathTraverserConfig {

    public final WrittenEventProvider writtenEventProvider;
    public final CommitChange commitChange;
    public final String viewIdFieldName;
    public final boolean notificationRequired;

    public PathTraverserConfig(
            WrittenEventProvider writtenEventProvider,
            CommitChange commitChange,
            String viewIdFieldName,
            boolean idCentric,
            boolean notificationRequired) {
        this.writtenEventProvider = writtenEventProvider;
        this.commitChange = commitChange;
        this.viewIdFieldName = viewIdFieldName;
        this.notificationRequired = notificationRequired;
    }

}

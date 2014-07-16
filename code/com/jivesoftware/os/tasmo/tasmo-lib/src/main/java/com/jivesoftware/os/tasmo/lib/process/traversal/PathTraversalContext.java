/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.tasmo.lib.write.ViewField;
import java.util.ArrayList;
import java.util.List;

/**
 * Context used while processing an event for a specific model path. Passed to ChainableProcessSteps which implement the operations performed at each step in
 * the path.
 */
public class PathTraversalContext {

    private final long threadTimestamp;
    private final boolean removalContext;
    private final List<ViewField> changes = new ArrayList<>();

    public PathTraversalContext(long threadTimestamp,
            boolean removalContext) {
        this.threadTimestamp = threadTimestamp;
        this.removalContext = removalContext;
    }

    public boolean isRemovalContext() {
        return removalContext;
    }

    public long getThreadTimestamp() {
        return threadTimestamp;
    }

    public List<ViewField> takeChanges() {
        List<ViewField> take = new ArrayList<>(changes);
        changes.clear();
        return take;
    }

    void addChange(ViewField update) {
        changes.add(update);
    }

}

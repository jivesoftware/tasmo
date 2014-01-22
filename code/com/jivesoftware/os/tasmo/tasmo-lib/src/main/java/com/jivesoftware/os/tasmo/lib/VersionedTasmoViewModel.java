/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.EventProcessorDispatcher;

/**
 *
 * @author jonathan.colt
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final ListMultimap<String, EventProcessorDispatcher> dispatchers;

    public VersionedTasmoViewModel(ChainedVersion version,
        ListMultimap<String, EventProcessorDispatcher> dispatchers) {
        this.version = version;
        this.dispatchers = dispatchers;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public ListMultimap<String, EventProcessorDispatcher> getDispatchers() {
        return dispatchers;
    }

    @Override
    public String toString() {
        return "VersionedViewTasmoModel{"
            + "version=" + version
            + ", dispatchers=" + dispatchers
            + '}';
    }
}

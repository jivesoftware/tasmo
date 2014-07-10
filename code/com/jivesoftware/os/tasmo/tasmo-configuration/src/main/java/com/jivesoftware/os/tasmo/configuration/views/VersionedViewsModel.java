/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration.views;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import java.util.Collections;
import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
class VersionedViewsModel {

    private final ChainedVersion version;
    private final Map<String, Map<Long, PathAndDictionary>> viewValueBindings;

    VersionedViewsModel(ChainedVersion version) {
        this(version, Collections.<String, Map<Long, PathAndDictionary>>emptyMap());
    }

    VersionedViewsModel(ChainedVersion version, Map<String, Map<Long, PathAndDictionary>> viewValueBindings) {
        this.version = Preconditions.checkNotNull(version);
        this.viewValueBindings = Preconditions.checkNotNull(viewValueBindings);
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public Map<String, Map<Long, PathAndDictionary>> getViewsModel() {
        return Collections.unmodifiableMap(viewValueBindings);
    }

    @Override
    public String toString() {
        return "VersionedEventsModel{" + "version=" + version + ", viewValueBindings=" + viewValueBindings + '}';
    }
}

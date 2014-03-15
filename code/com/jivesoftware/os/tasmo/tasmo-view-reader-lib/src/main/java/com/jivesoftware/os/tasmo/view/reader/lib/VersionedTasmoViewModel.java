/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import java.util.Map;

/**
 *
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final Map<String, ViewBinding> bindings;

    public VersionedTasmoViewModel(ChainedVersion version,
        Map<String, ViewBinding> bindings) {
        this.version = version;
        this.bindings = bindings;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public ViewBinding getBinding(String viewClassName) {
        return bindings.get(viewClassName);
    }

    @Override
    public String toString() {
        return "VersionedViewTasmoModel{"
            + "version=" + version
            + ", bindings=" + bindings
            + '}';
    }
}

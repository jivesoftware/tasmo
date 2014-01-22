/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore.ViewCollector;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class ViewValueReader {

    private final ViewValueStore viewValueStore;

    public ViewValueReader(ViewValueStore viewValueStore) {
        this.viewValueStore = viewValueStore;
    }

    public void readViewValues(List<? extends ViewCollector> viewCollectors) throws IOException {
        viewValueStore.multiGet(viewCollectors);
    }
}

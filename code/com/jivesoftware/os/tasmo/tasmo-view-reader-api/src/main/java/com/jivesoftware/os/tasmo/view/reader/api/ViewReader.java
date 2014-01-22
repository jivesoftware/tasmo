/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.view.reader.api;

import java.io.IOException;
import java.util.List;

/**
 * Provides methods for reading views
 */
public interface ViewReader<V> {

    public static long NULL_ACTOR_ID = 0;

    /**
     * Reads one specific view.
     *
     * @param viewRequest descriptor representing the view to read
     * @return
     * @throws IOException, ViewReaderException
     */
    V readView(ViewDescriptor viewRequest) throws IOException, ViewReaderException;

    /**
     * Retrieves multiple views with a single request
     *
     * @param view list of descriptors representing the views to read
     * @return
     * @throws IOException, ViewReaderException
     */
    List<V> readViews(List<ViewDescriptor> viewRequests) throws IOException, ViewReaderException;
}

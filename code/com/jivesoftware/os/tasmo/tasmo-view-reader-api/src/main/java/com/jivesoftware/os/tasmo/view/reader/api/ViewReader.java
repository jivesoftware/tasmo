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
 * @param <V>
 */
public interface ViewReader<V> {

    public static long NULL_ACTOR_ID = 0;

    /**
     * Reads one specific view.
     *
     * @param viewRequest descriptor representing the view to read
     * @return
     * @throws IOException, ViewReaderException
     * @throws com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException
     */
    V readView(ViewDescriptor viewRequest) throws IOException, ViewReaderException;

    /**
     * Retrieves multiple views with a single request
     *
     * @param viewRequests
     * @return
     * @throws IOException, ViewReaderException
     * @throws com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException
     */
    List<V> readViews(List<ViewDescriptor> viewRequests) throws IOException, ViewReaderException;
}

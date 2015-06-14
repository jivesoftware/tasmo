package com.jivesoftware.os.tasmo.view.reader.api;

import java.io.IOException;
import java.util.List;

/**
 * Provides methods for reading views
 * @param <V>
 */
public interface ViewReader<V> {

    public static long NULL_ACTOR_ID = 0; // TODO kill this constant and its usages!!

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

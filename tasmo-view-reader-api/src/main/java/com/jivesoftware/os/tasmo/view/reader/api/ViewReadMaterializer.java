package com.jivesoftware.os.tasmo.view.reader.api;

import java.io.IOException;
import java.util.List;

/**
 * Provides methods for reading views
 * @param <V>
 */
public interface ViewReadMaterializer<V> {

    /**
     * Builds one view just in time from the latest state on the callers thread.
     *
     * @param viewRequest descriptor representing the view to read
     * @return
     * @throws IOException, ViewReaderException
     * @throws com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException
     */
    V readMaterializeView(ViewDescriptor viewRequest) throws IOException, ViewReaderException;

    /**
     * Builds multiple views just in time from the latest state on the callers thread.
     *
     * @param viewRequests
     * @return
     * @throws IOException, ViewReaderException
     * @throws com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException
     */
    List<V> readMaterializeViews(List<ViewDescriptor> viewRequests) throws IOException, ViewReaderException;
}

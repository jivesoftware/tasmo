package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;


/**
 *
 */
public class LocalMaterializationSystem {

    private final EventWriter writer;
    private final ViewReader<ViewResponse> reader;
    private final ShutdownCallback shutdownCallback;

    LocalMaterializationSystem(EventWriter writer, ViewReader<ViewResponse> reader, ShutdownCallback shutdownCallback) {
        this.writer = writer;
        this.reader = reader;
        this.shutdownCallback = shutdownCallback;
    }

    public ViewReader<ViewResponse> getReader() {
        return reader;
    }

    public EventWriter getWriter() {
        return writer;
    }

    public void shutDown() {
        shutdownCallback.onShutDown();
    }

    interface ShutdownCallback {

        void onShutDown();
    }
}

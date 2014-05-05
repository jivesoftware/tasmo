package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.id.IdProvider;
import com.jivesoftware.os.tasmo.id.IdProviderImpl;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;

/**
 *
 */
public class LocalMaterializationSystem {

    private final EventWriter writer;
    private final ViewReader<ViewResponse> reader;
    private final IdProvider idProvider;
    private final ShutdownCallback shutdownCallback;

    LocalMaterializationSystem(EventWriter writer, ViewReader<ViewResponse> reader, OrderIdProvider orderIdProvider,
        ShutdownCallback shutdownCallback) {
        this.writer = writer;
        this.reader = reader;
        this.idProvider = new IdProviderImpl(orderIdProvider);
        this.shutdownCallback = shutdownCallback;
    }

    public ViewReader<ViewResponse> getReader() {
        return reader;
    }

    public EventWriter getWriter() {
        return writer;
    }

    public IdProvider getIdProvider() {
        return idProvider;
    }

    public void shutDown() {
        shutdownCallback.onShutDown();
    }

    interface ShutdownCallback {

        void onShutDown();
    }
}

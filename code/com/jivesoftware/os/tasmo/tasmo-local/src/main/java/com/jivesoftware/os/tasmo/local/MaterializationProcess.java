package com.jivesoftware.os.tasmo.local;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventWriteException;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.id.BaseView;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewId;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReaderException;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;

import java.io.IOException;

/**
 *
 */
public class MaterializationProcess {

    private final LocalMaterializationSystem localMaterializationSystem;
    private final JsonEventConventions jsonEventConventions = new JsonEventConventions();

    public MaterializationProcess(LocalMaterializationSystem localMaterializationSystem) {
        this.localMaterializationSystem = localMaterializationSystem;
    }

        public void writeAncillaryEvents(Event... ancillaryEvents) throws EventWriteException {
        EventWriter writer = localMaterializationSystem.getWriter();

        if (ancillaryEvents != null) {
            writer.write(ancillaryEvents);
        }
    }

    public <V extends BaseView<?>> V readView(Event rootEvent, Class<V> viewClass) throws IOException, ViewReaderException {
        ViewReader<ViewResponse> reader = localMaterializationSystem.getReader();

        ViewId<V> viewId = ViewId.ofObject(rootEvent.getObjectId(), viewClass);

        ObjectNode eventJson = rootEvent.toJson();
        TenantId tenant = jsonEventConventions.getTenantId(eventJson);
        Id actorId = jsonEventConventions.getActor(eventJson);
        Id centricId = jsonEventConventions.getUserId(eventJson);

        ViewResponse viewResponse;

        if (actorId.equals(centricId)) {
            viewResponse = reader.readView(new ViewDescriptor(tenant, actorId, viewId));
        } else {
            viewResponse = reader.readView(new ViewDescriptor(tenant, actorId, viewId, centricId));
        }

        if (viewResponse.getStatusCode() == ViewResponse.StatusCode.OK) {
            return viewResponse.getView(viewClass);
        } else {
            return null;
        }
    }

    public <V extends BaseView> V writeEventAndReadView(Event rootEvent, Class<V> viewClass)
        throws EventWriteException, IOException, ViewReaderException {
        return writeEventsAndReadView(rootEvent, viewClass, (Event[]) null);
    }

    public <V extends BaseView<?>> V writeEventsAndReadView(Event rootEvent, Class<V> viewClass, Event... ancillaryEvents)
        throws EventWriteException, IOException, ViewReaderException {
        EventWriter writer = localMaterializationSystem.getWriter();
        ViewReader<ViewResponse> reader = localMaterializationSystem.getReader();

        writer.write(rootEvent);
        if (ancillaryEvents != null) {
            writer.write(ancillaryEvents);
        }

        ViewId<V> viewId = ViewId.ofObject(rootEvent.getObjectId(), viewClass);

        ObjectNode eventJson = rootEvent.toJson();
        TenantId tenant = jsonEventConventions.getTenantId(eventJson);
        Id actorId = jsonEventConventions.getActor(eventJson);
        Id centricId = jsonEventConventions.getUserId(eventJson);

        ViewResponse viewResponse;

        if (actorId.equals(centricId)) {
            viewResponse = reader.readView(new ViewDescriptor(tenant, actorId, viewId));
        } else {
            viewResponse = reader.readView(new ViewDescriptor(tenant, actorId, viewId, centricId));
        }

        if (viewResponse.getStatusCode() == ViewResponse.StatusCode.OK) {
            return viewResponse.getView(viewClass);
        } else {
            return null;
        }
    }
}
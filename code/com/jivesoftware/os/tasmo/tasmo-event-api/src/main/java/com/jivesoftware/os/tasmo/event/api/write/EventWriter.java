package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import java.util.Arrays;
import java.util.List;

/**
 *
 *
 */
public class EventWriter {
    private final JsonEventConventions jsonEventConventions = new JsonEventConventions();
    private final JsonEventWriter jsonEventWriter;

    public EventWriter(JsonEventWriter jsonEventWriter) {
        this.jsonEventWriter = jsonEventWriter;
    }

    public EventWriterResponse write(Event ... event) throws EventWriteException {
        return write(Arrays.asList(event), EventWriterOptions.defaultOptions());
    }

    public EventWriterResponse write(List<Event> events) throws EventWriteException {
        return write(events, EventWriterOptions.defaultOptions());
    }

    public EventWriterResponse write(List<Event> events, final EventWriterOptions options) throws EventWriteException {
        try {
            return jsonEventWriter.write(Lists.transform(events, new Function<Event, ObjectNode>() {
                @Override
                public ObjectNode apply(Event input) {
                    ObjectNode objectNode = input.toJson();
                    if (options.isInFlightTrackingEnabled()) {
                        jsonEventConventions.setTrackEventProcessedLifecycle(objectNode, Boolean.TRUE);
                    }
                    return objectNode;
                }
            }), options);
        } catch (JsonEventWriteException e) {
            e.printStackTrace();
            throw new EventWriteException(e);
        }
    }
}

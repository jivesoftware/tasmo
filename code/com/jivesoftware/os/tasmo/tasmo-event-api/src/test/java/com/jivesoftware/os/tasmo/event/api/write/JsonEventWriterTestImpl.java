package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.util.ArrayList;
import java.util.List;

public class JsonEventWriterTestImpl implements JsonEventWriter {

    private static final JsonEventConventions jsonEventConventions = new JsonEventConventions();
    private final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(1);

    @Override
    public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException {

        try {
            List<ObjectId> objectIds = new ArrayList<>();
            List<Long> eventIds = new ArrayList<>();
            for (ObjectNode w : events) {
                String className = jsonEventConventions.getInstanceClassName(w);
                ObjectId objectId = new ObjectId(className, jsonEventConventions.getInstanceId(w, className));
                eventIds.add(orderIdProvider.nextId());
                objectIds.add(objectId);
            }
            return new EventWriterResponse(eventIds, objectIds);
        } catch (Exception ex) {
            throw new JsonEventWriteException("sad trombone", ex);
        }
    }

}

package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.tasmo.event.api.JsonEventConventions;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 *
 */
public class MockJsonEventWriter implements JsonEventWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final JsonEventConventions jsonEventConventions = new JsonEventConventions();
    private final ObjectMapper mapper = new ObjectMapper();
    private final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(1);
    private final AtomicReference<List<ObjectNode>> events = new AtomicReference<>();

    @Override
    public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException {
        Preconditions.checkNotNull(events);
        ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
        List<Long> eventIds = new ArrayList<>(events.size());
        List<ObjectId> objectIds = new ArrayList<>(events.size());
        List<ObjectNode> responseList = this.events.get();
        for (ObjectNode event : events) {
            if (LOG.isDebugEnabled()) {
                try {
                    LOG.debug("Mock received event: " + writer.writeValueAsString(event));
                } catch (IOException e) {
                    throw new JsonEventWriteException("Failed to serialize event", e);
                }
            }
            eventIds.add(orderIdProvider.nextId());
            objectIds.add(jsonEventConventions.getInstanceObjectId(event, jsonEventConventions.getInstanceClassName(event)));
            if (responseList != null) {
                responseList.add(event);
            }
        }
        return new EventWriterResponse(eventIds, objectIds);
    }

    public void enableEventCapture() {
        List<ObjectNode> eventList = events.get();
        if (eventList == null) {
            eventList = new ArrayList<>();
            events.compareAndSet(null, eventList);
        }
    }

    public List<ObjectNode> getCapturedEvents() {
        return events.getAndSet(new ArrayList<ObjectNode>());
    }

}

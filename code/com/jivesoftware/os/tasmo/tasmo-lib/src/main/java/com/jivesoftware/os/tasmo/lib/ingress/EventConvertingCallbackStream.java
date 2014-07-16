package com.jivesoftware.os.tasmo.lib.ingress;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pete
 */
public class EventConvertingCallbackStream implements CallbackStream<List<ObjectNode>> {

    private final CallbackStream<List<WrittenEvent>> eventIngressCallbackStream;
    private final WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider;

    public EventConvertingCallbackStream(CallbackStream<List<WrittenEvent>> eventIngressCallbackStream,
        WrittenEventProvider<ObjectNode, JsonNode> writtenEventProvider) {
        this.eventIngressCallbackStream = eventIngressCallbackStream;
        this.writtenEventProvider = writtenEventProvider;
    }

    @Override
    public List<ObjectNode> callback(List<ObjectNode> objectNodes) throws Exception {
        if (objectNodes != null) {
            List<WrittenEvent> converted = new ArrayList<>(objectNodes.size());
            for (ObjectNode objectNode : objectNodes) {
                converted.add(writtenEventProvider.convertEvent(objectNode));
            }
            List<WrittenEvent> failed = eventIngressCallbackStream.callback(converted);
            List<ObjectNode> failedNodes = new ArrayList<>();
            for (WrittenEvent f : failed) {
                for (int i = 0; i < converted.size(); i++) {
                    if (converted.get(i) == f) {
                        failedNodes.add(objectNodes.get(i));
                        break;
                    }
                }
            }
            return failedNodes;

        } else {
            eventIngressCallbackStream.callback(null);
            return objectNodes;
        }

    }
}

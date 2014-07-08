package com.jivesoftware.os.tasmo.configuration.events;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.configuration.EventModel;
import com.jivesoftware.os.tasmo.configuration.ValueType;
import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
public class EventValidator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final boolean failInvalidEvents;

    public EventValidator(boolean failInvalidEvents) {
        this.failInvalidEvents = failInvalidEvents;
    }

    /**
     *
     * @param eventsModel model to validate against, or {@code null}
     * @param eventNode event to validate
     * @return validation result
     */
    public Validated validateEvent(VersionedEventsModel eventsModel, ObjectNode eventNode) {
        final EventModel ingressEvent = EventModel.builder(eventNode, false).build();
        Validated.ValidatedBuilder builder = Validated.build();
        LOG.inc("validated_events");
        if (eventsModel == null || eventsModel.getEventsModel() == null) {
            LOG.inc("no_model");
            builder.addMessage(failInvalidEvents, "There is no event model to validate against. Please call loadModel()");
        } else {
            builder.setVersion(eventsModel.getVersion());
            EventModel modelEvent = eventsModel.getEventsModel().getEvent(ingressEvent.getEventClass());

            if (modelEvent == null) {
                LOG.inc("unknown_event>" + ingressEvent.getEventClass());
                LOG.inc("unknown_event");
                builder.addMessage(failInvalidEvents,
                        "'" + ingressEvent.getEventClass() + "' is not declared in the current event model. Do you need to reload the model?");
            } else {
                Map<String, ValueType> modelFields = modelEvent.getEventFields();
                Map<String, ValueType> ingressFields = ingressEvent.getEventFields();

                for (Map.Entry<String, ValueType> ingressField : ingressFields.entrySet()) {
                    String ingressFieldName = ingressField.getKey();
                    ValueType ingressFieldType = ingressField.getValue();
                    if (ingressFieldType != ValueType.unknown) {
                        ValueType modelFieldType = modelFields.get(ingressFieldName);
                        if (modelFieldType == null) {
                            LOG.inc("unexpected_field>" + ingressFieldName);
                            LOG.inc("unexpected_field");
                            builder.addMessage(failInvalidEvents, "unexpected: " + ingressFieldType);
                        }
                        if (modelFieldType != ingressField.getValue()) {
                            LOG.inc("unexpected_type>" + ingressFieldName + ">" + ingressFieldType);
                            LOG.inc("unexpected_type");
                            builder.addMessage(failInvalidEvents,
                                    "expected: '" + ingressFieldName + "' of type '" + modelFieldType
                                            + "' was expected to be of type '" + ingressFieldType + "'");
                        }
                    }
                }
            }
        }

        return builder.build();
    }
}

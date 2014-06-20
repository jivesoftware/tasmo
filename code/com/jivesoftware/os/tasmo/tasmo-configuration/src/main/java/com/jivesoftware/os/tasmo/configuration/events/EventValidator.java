package com.jivesoftware.os.tasmo.configuration.events;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.configuration.EventModel;
import com.jivesoftware.os.tasmo.configuration.ValueType;
import java.util.ArrayList;
import java.util.List;
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
     * @param event
     * @return
     */
    public Validated validateEvent(VersionedEventsModel eventsModel, ObjectNode eventNode) {
        final EventModel ingressEvent = EventModel.builder(eventNode, false).build();
        if (eventsModel == null || eventsModel.getEventsModel() == null) {
            if (failInvalidEvents) {
                LOG.error("There is no model to validate against. Please call loadModel()");
                return Validated.build().invalid();
            } else {
                LOG.warn("There is no model to validate against. Please call loadModel()");
                throw new RuntimeException("There is no model to validate against. Please call loadModel()");
            }
        } else {
            EventModel modelEvent = eventsModel.getEventsModel().getEvent(ingressEvent.getEventClass());
            if (modelEvent == null) {
                if (failInvalidEvents) {
                    LOG.error(ingressEvent.getEventClass() + " is not declared in the current model. Do you need to reload the model?");
                    return Validated.build()
                        .setVersion(eventsModel.getVersion())
                        .setUnexpectedFields(ingressEvent.getEventFields().keySet())
                        .invalid();
                } else {
                    LOG.warn(ingressEvent.getEventClass() + " is not declared in the current model. Do you need to reload the model?");
                    return Validated.build().valid();
                }
            }
            final List<String> unexpectedFields = new ArrayList<>();
            Map<String, ValueType> modelFields = modelEvent.getEventFields();
            Map<String, ValueType> ingressFields = ingressEvent.getEventFields();

            for (Map.Entry<String, ValueType> ingressField : ingressFields.entrySet()) {
                String ingressFieldName = ingressField.getKey();
                ValueType ingressFieldType = ingressField.getValue();
                if (ingressFieldType != ValueType.unknown) {
                    ValueType modelFieldType = modelFields.get(ingressFieldName);
                    if (modelFieldType == null) {
                        unexpectedFields.add("unexpected: " + ingressFieldType + " " + ingressFieldName);
                    }
                    if (modelFieldType != ingressField.getValue()) {
                        unexpectedFields.add("expected:" + modelFieldType + " " + ingressFieldName + " but was " + ingressFieldType + " "
                            + ingressFieldName);
                    }
                }
            }

            Validated.ValidatedBuilder validatedBuilder = Validated.build()
                .setVersion(eventsModel.getVersion())
                .setUnexpectedFields(unexpectedFields);
            if (unexpectedFields.isEmpty()) {
                return validatedBuilder.valid();
            } else {
                if (failInvalidEvents) {
                    return validatedBuilder.invalid();
                } else {
                    return validatedBuilder.valid();
                }
            }
        }
    }
}

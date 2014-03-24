/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration.events;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventFieldValueType;
import com.jivesoftware.os.tasmo.model.VersionedEventsModel;
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
        final EventDefinition ingressEvent = EventDefinition.builder(eventNode, false).build();
        if (eventsModel == null || eventsModel.getEventsModel() == null) {
            if (failInvalidEvents) {
                LOG.error("There is no model to validate against. Please call loadModel()");
                return Validated.build().invalid();
            } else {
                LOG.warn("There is no model to validate against. Please call loadModel()");
                throw new RuntimeException("There is no model to validate against. Please call loadModel()");
            }
        } else {
            EventDefinition modelEvent = eventsModel.getEventsModel().getEvent(ingressEvent.getEventClass());
            if (modelEvent == null) {
                if (failInvalidEvents) {
                    LOG.error(ingressEvent.getEventClass() + " is not declared in the current model. Do you need to reload the mode?");
                    return Validated.build()
                        .setVersion(eventsModel.getVersion())
                        .setUnexpectedFields(ingressEvent.getEventFields().keySet())
                        .invalid();
                } else {
                    LOG.warn(ingressEvent.getEventClass() + " is not declared in the current model. Do you need to reload the mode?");
                    return Validated.build().valid();
                }
            }
            final List<String> unexpectedFields = new ArrayList<>();
            Map<String, EventFieldValueType> modelFields = modelEvent.getEventFields();
            Map<String, EventFieldValueType> ingressFields = ingressEvent.getEventFields();

            for (Map.Entry<String, EventFieldValueType> ingressField : ingressFields.entrySet()) {
                String ingressFieldName = ingressField.getKey();
                EventFieldValueType ingressFieldType = ingressField.getValue();
                if (ingressFieldType != EventFieldValueType.unknown) {
                    EventFieldValueType modelFieldType = modelFields.get(ingressFieldName);
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

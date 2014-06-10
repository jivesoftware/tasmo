/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class ValidatingPathCallback implements PathCallback {

    private final EventsModel eventsModel;
    private final PathCallback pathCallback;

    public ValidatingPathCallback(EventsModel eventsModel, PathCallback pathCallback) {
        this.eventsModel = eventsModel;
        this.pathCallback = pathCallback;
    }

    private void assertFieldIsPresent(Set<String> classNames, ValueType type, String[] fieldNames) throws IllegalArgumentException {
        if (type != ValueType.backrefs && type != ValueType.latest_backref && type != ValueType.count) {
            for (String fieldName : fieldNames) {
                int foundEventsWithField = 0;
                if (isIgnoredField(fieldName)) {
                    continue;
                }
                for (String className : classNames) {
                    EventModel eventConfiguration = eventsModel.getEvent(className);
                    Map<String, ValueType> event = eventConfiguration.getEventFields();
                    if (event != null && event.containsKey(fieldName)) {
                        if (event.get(fieldName).equals(type)) {
                            foundEventsWithField++;
                        } else {
                            if (type == ValueType.value) {
                                if (event.get(fieldName).equals(ValueType.ref)) {
                                    foundEventsWithField++;
                                } else if (event.get(fieldName).equals(ValueType.refs)) {
                                    foundEventsWithField++;
                                }
                            }
                        }
                    }

                }
                if (foundEventsWithField == 0) {
                    String msg = "The view expects one of these events:" + classNames + " to have field:" + fieldName + " and be of type:" + type;
                    throw new IllegalArgumentException(msg);
                }
            }
        }
    }

    private boolean isIgnoredField(String fieldName) {
        return ReservedFields.NIL_FIELD.equals(fieldName);
    }

    @Override
    public void push(Set<String> fieldType, ValueType valueType, String... fieldNames) {
        assertFieldIsPresent(fieldType, valueType, fieldNames);
        pathCallback.push(fieldType, valueType, fieldNames);
    }

    @Override
    public void pop() {
        pathCallback.pop();
    }
}

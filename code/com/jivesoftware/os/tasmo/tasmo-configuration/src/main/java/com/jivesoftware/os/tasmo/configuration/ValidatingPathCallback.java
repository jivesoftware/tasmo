/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

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
                for (String className : classNames) {
                    EventModel eventConfiguration = eventsModel.getEvent(className);
                    Map<String, ValueType> event = eventConfiguration.getEventFields();
                    if (event != null && event.containsKey(fieldName)) {
                        if (event.get(fieldName).equals(type)) {
                            continue;
                        } else {
                            if (type == ValueType.value) {
                                if (event.get(fieldName).equals(ValueType.ref)) {
                                    continue;
                                }
                                if (event.get(fieldName).equals(ValueType.refs)) {
                                    continue;
                                }
                            }
                        }
                    }

                    String msg = "The view expects event:" + className + " to have field:" + fieldName + " and be of type:" + type + "."
                            + " found: event:" + ((event == null) ? null : className)
                            + " field:" + ((event == null || !event.containsKey(fieldName)) ? null : fieldName)
                            + " type:" + ((event == null) ? null : event.get(fieldName));
                    throw new IllegalArgumentException(msg);
                }
            }
        }
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

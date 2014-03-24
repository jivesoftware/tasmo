/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventFieldValueType;
import com.jivesoftware.os.tasmo.model.EventsModel;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
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

    private void assertFieldIsPresent(Set<String> classNames, ModelPathStepType type, String[] fieldNames) throws IllegalArgumentException {
        if (type != ModelPathStepType.backRefs && type != ModelPathStepType.latest_backRef && type != ModelPathStepType.count) {
            for (String fieldName : fieldNames) {
                for (String className : classNames) {
                    EventDefinition eventConfiguration = eventsModel.getEvent(className);
                    Map<String, EventFieldValueType> event = eventConfiguration.getEventFields();
                    if (event != null && event.containsKey(fieldName)) {
                        EventFieldValueType eventFieldType = event.get(fieldName);
                        if (eventFieldType == EventFieldValueType.ref && type == ModelPathStepType.ref) {
                            continue;
                        } else if (eventFieldType == EventFieldValueType.refs && type == ModelPathStepType.refs) {
                            continue;
                        } else if (eventFieldType == EventFieldValueType.value && type == ModelPathStepType.value) {
                            continue;
                        } else if (type == ModelPathStepType.value) {  {
                                if (event.get(fieldName).equals(EventFieldValueType.ref)) {
                                    continue;
                                }
                                if (event.get(fieldName).equals(EventFieldValueType.refs)) {
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
        public void push(Set<String> fieldType, ModelPathStepType valueType, String... fieldNames) {
        assertFieldIsPresent(fieldType, valueType, fieldNames);
        pathCallback.push(fieldType, valueType, fieldNames);
    }

    @Override
    public void pop() {
        pathCallback.pop();
    }
}

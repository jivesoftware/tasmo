/*
 * Copyright 2014 pete.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.tasmo.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 */
public class EventModelParser {

    public EventsModel parse(String eventsModel) {
        StringTokenizer tokenizer = new StringTokenizer(eventsModel, "|");
        EventsModel model = new EventsModel();

        while (tokenizer.hasMoreTokens()) {
            String eventDef = tokenizer.nextToken();
            String[] nameAndFields = eventDef.split(":");
            if (nameAndFields.length != 2) {
                throw new IllegalArgumentException();
            }

            Map<String, ValueType> fields = new HashMap<>();

            for (String fieldDef : nameAndFields[1].split(",")) {
                int idx = fieldDef.indexOf('(');
                if (idx < 0 || !fieldDef.endsWith(")")) {
                    throw new IllegalArgumentException("Field definitions require the form name(type)");
                }
                String fieldName = fieldDef.substring(0, idx);
                String fieldType = fieldDef.substring(idx + 1, fieldDef.indexOf(')'));

                fields.put(fieldName, ValueType.valueOf(fieldType));
            }

            model.addEvent(new EventModel(nameAndFields[0], fields));
        }

        return model;

    }
}

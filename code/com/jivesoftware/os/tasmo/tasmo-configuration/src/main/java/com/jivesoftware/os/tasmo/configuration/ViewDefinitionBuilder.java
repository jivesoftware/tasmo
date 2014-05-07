/*
 * Copyright 2014 jive software.
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

import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Allows construction of a view definition given simple string representations of model paths. The model path strings need to be in the form [event class
 * name].[event field name]|[event class name].[event field name] A single class/field pair would select values of the root object. Pipe delimiters between
 * pairs are used when reference fields are traversed.
 *
 * For example Document.author|User.userName would traverse any reference field named author of a Document event type, select the userName field of the User
 * object on the far side of that reference.
 *
 * Back references are indicated by pulling the reference field from the referrer to the referred type. For example, reversing the previous path to select the
 * title of all Documents which refer to a specific User via the Document.author field would look like this: User.author|Document.title. Note the author field
 * of Document is pulled into the pair for the User portion of the path even though the User event has no author field.
 *
 * Different back-reference types can be indicated by putting qualifiers in parenthesis. The default is all backrefs, which pulls in all referencing objects in
 * an array. The latest referencer can be requested by: User.author(latest)|Document.title and the count of referencers can be requested by
 * User.author(count)|Document.title. In this last example, which field of the document type is indicated doesn't matter since values of the referencing
 * Documents themselves won't appear in the view, just how many referencers there are. When in doubt here you can use instanceId, which is common to all event
 * types.
 *
 * Multiple value fields at the tail of a path can be selected by separating them with commas. Document.title,body,modDate
 *
 * Since views have a single root type, all model path strings need to start with the root event type.
 */
public class ViewDefinitionBuilder {

    private final EventsModel eventsModel;
    private final String viewName;
    private final String rootEventType;
    private final List<ModelPath> paths;
    private boolean idCentric;
    private boolean notifiable;

    public ViewDefinitionBuilder(EventsModel eventsModel, String viewName, String rootEventType) {
        this.eventsModel = eventsModel;
        this.viewName = viewName;
        this.rootEventType = rootEventType;
        this.paths = new ArrayList<>();
    }

    public void setIdCentric(boolean idCentric) {
        this.idCentric = idCentric;
    }

    public void setNotifiable(boolean notifiable) {
        this.notifiable = notifiable;
    }

    public void addPath(String modelPathString) {

        if (!modelPathString.startsWith(rootEventType)) {
            throw new IllegalArgumentException(modelPathString + " does not start with the root event type of " + rootEventType);
        }

        List<PathElement> elements = new ArrayList<>();
        List<ModelPathStep> pathSteps = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(modelPathString, "|");

        while (tokenizer.hasMoreTokens()) {
            elements.add(new PathElement(tokenizer.nextToken()));
        }

        for (int pathIdx = 0; pathIdx < elements.size(); pathIdx++) {
            PathElement element = elements.get(pathIdx);

            Map<String, ValueType> eventFields = element.eventModel.getEventFields();

            if (pathIdx == elements.size() - 1) {
                for (String field : element.fields) {
                    if (eventFields.get(field) != ValueType.value) {
                        throw new IllegalArgumentException(field + " is not a value field of " + element.eventClass);
                    }
                }
                pathSteps.add(new ModelPathStep(pathIdx == 0, element.eventClass, Arrays.<String>asList(element.fields)));

            } else {
                if (element.fields.length > 1) {
                    throw new IllegalArgumentException("Multiple fields can only be selected at the tail of the path. "
                        + element.eventClass + "violates this.");
                }

                ModelPathStep step;
                ValueType valueType = eventFields.get(element.fields[0]);

                if (valueType == null) {
                    step = handleBackref(pathIdx == 0, element.eventClass, element.fields[0], element.qualifier, elements.get(pathIdx + 1));
                } else if (valueType == ValueType.ref) {
                    String destination = elements.get(pathIdx + 1).eventClass;
                    step = new ModelPathStep(pathIdx == 0, element.eventClass, element.fields[0], ModelPathStepType.ref, destination);
                } else if (valueType == ValueType.refs) {
                    String destination = elements.get(pathIdx + 1).eventClass;
                    step = new ModelPathStep(pathIdx == 0, element.eventClass, element.fields[0], ModelPathStepType.refs, destination);
                } else {
                    throw new IllegalArgumentException("Unexpected value type " + valueType + " for event " + element.eventClass
                        + "and field " + element.fields[0]);
                }

                pathSteps.add(step);

            }
        }

        ModelPath.Builder builder = ModelPath.builder("" + paths.size() + 1);
        paths.add(builder.addPathMembers(pathSteps).build());

    }

    private ModelPathStep handleBackref(boolean head, String destination, String refField, String qualifier, PathElement nextElement) {
        EventModel sourceEvent = eventsModel.getEvent(nextElement.eventClass);

        if (qualifier != null) {
            refField = refField.substring(0, refField.indexOf(qualifier) - 1);
        }

        ValueType type = sourceEvent.getEventFields().get(refField);

        if (type == ValueType.ref || type == ValueType.refs) {
            if ("count".equals(qualifier)) {
                return new ModelPathStep(head, nextElement.eventClass, refField, ModelPathStepType.count, destination);
            } else if ("latest".equals(qualifier)) {
                return new ModelPathStep(head, nextElement.eventClass, refField, ModelPathStepType.latest_backRef, destination);
            } else {
                return new ModelPathStep(head, nextElement.eventClass, refField, ModelPathStepType.backRefs, destination);
            }
        }

        throw new IllegalArgumentException("event " + destination + " and field " + refField + " are not valid for the current model");
    }

    public ViewBinding build() {
        return new ViewBinding(viewName, paths, idCentric, notifiable);
    }

    private class PathElement {

        private final String eventClass;
        private final String[] fields;
        private final String qualifier;
        private final EventModel eventModel;

        private PathElement(String pathElement) {
            String[] eventAndField = pathElement.split("\\.");

            if (eventAndField.length != 2) {
                throw new IllegalArgumentException("Event field pairs need to be in the [form event type].[field name]");
            }

            this.fields = eventAndField[1].split(",");

            String qual = null;
            int qualStart = fields[0].indexOf("(");
            if (fields.length == 1 && qualStart > 0) {
                int qualEnd = fields[0].indexOf(")", qualStart);
                if (qualEnd > 0) {
                    qual = fields[0].substring(qualStart + 1, qualEnd);
                }
            }

            this.eventClass = eventAndField[0];
            this.qualifier = qual;

            EventModel model = eventsModel.getEvent(eventClass);
            if (model == null) {
                throw new IllegalArgumentException(eventClass + " is not a valid event type for the supplied event model");
            } else {
                this.eventModel = model;
            }
        }
    }
}

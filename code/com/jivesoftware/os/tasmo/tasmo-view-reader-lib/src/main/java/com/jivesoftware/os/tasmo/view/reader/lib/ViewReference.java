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
package com.jivesoftware.os.tasmo.view.reader.lib;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ViewReference {

    private final ModelPath path;
    private final ModelPathStep step;
    private final ObjectId origin;
    private final List<ObjectId> destinations;
    private final AtomicReference<Reference> latest;

    public ViewReference(ModelPath path, ModelPathStep step, ObjectId objectId) {
        if (ModelPathStepType.value.equals(step.getStepType())) {
            throw new IllegalArgumentException("ViewReference cannot be built with a value step");
        } else {
            this.path = path;
            this.step = step;
            this.origin = objectId;
            this.destinations = new ArrayList<>();
            this.latest = new AtomicReference<>();
        }
    }

    public ObjectId getOriginId() {
        return origin;
    }

    public boolean isBackReference() {
        return step.getStepType().isBackReferenceType();
    }

    public Set<String> getOriginClassNames() {
        return step.getOriginClassNames();
    }

    public String getRefFieldName() {
        return step.getRefFieldName();
    }

    public ModelPathStepType getStepType() {
        return step.getStepType();
    }

    public void addDestinationId(Reference destinationRef) {
        ObjectId objectId = destinationRef.getObjectId();
        if (ModelPathStepType.latest_backRef.equals(step.getStepType())) {
            Reference existing = latest.get();
            if (existing == null || destinationRef.getTimestamp() >= existing.getTimestamp()) {
                latest.set(destinationRef);
            }
        } else {
            destinations.add(objectId);
        }
    }

    public List<ObjectId> getDestinationIds() {
        Reference latestRef = latest.get();
        if (latestRef != null) {
            return Arrays.asList(latestRef.getObjectId());
        } else {
            return destinations;
        }
    }

    public ModelPath getPath() {
        return path;
    }
}

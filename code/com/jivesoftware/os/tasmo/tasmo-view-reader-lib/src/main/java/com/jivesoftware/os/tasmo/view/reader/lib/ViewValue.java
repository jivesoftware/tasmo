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
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ViewValue {

    private final ModelPath path;
    private final ModelPathStep step;
    private final ObjectId objectId;
    private final Map<String, OpaqueFieldValue> result;

    public ViewValue(ModelPath path, ModelPathStep step, ObjectId objectId) {
        if (ModelPathStepType.value.equals(step.getStepType())) {
            this.path = path;
            this.step = step;
            this.objectId = objectId;
            this.result = new HashMap<>();
        } else {
            throw new IllegalArgumentException("ViewValue can only be built with a value step");
        }

    }

    public ObjectId getObjectId() {
        return objectId;
    }

    public String[] getValueFieldNames() {
        Set<String> fieldNames = new HashSet<>(step.getFieldNames());
        return fieldNames.toArray(new String[fieldNames.size()]);
    }

    public void addResult(String fieldName, OpaqueFieldValue value) {
        result.put(fieldName, value);
    }

    public ModelPath getPath() {
        return path;
    }

    public Map<String, OpaqueFieldValue> getResult() {
        return result;
    }
}

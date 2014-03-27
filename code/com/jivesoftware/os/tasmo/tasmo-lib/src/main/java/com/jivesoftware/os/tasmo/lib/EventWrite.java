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
package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.Collection;
import java.util.Objects;

/**
 *
 * @author pete
 */
public class EventWrite {

    private final WrittenEvent writtenEvent;
    private final Multimap<ReferencedObjectKey, ObjectId> dereferencedObjects = ArrayListMultimap.create();
    private final Multimap<ReferencedObjectKey, ObjectId> referencedObjects = ArrayListMultimap.create();

    public EventWrite(WrittenEvent writtenEvent) {
        this.writtenEvent = writtenEvent;
    }

    public WrittenEvent getWrittenEvent() {
        return writtenEvent;
    }

    public void addDereferencedObjects(Id centricId, String refField, Collection<ObjectId> objectIds) {
        dereferencedObjects.putAll(new ReferencedObjectKey(refField, centricId), objectIds);
    }

    public void addDereferencedObject(Id centricId, String refField, ObjectId objectId) {
        dereferencedObjects.put(new ReferencedObjectKey(refField, centricId), objectId);
    }

    public void addReferencedObjects(Id centricId, String refField, Collection<ObjectId> objectIds) {
        referencedObjects.putAll(new ReferencedObjectKey(refField, centricId), objectIds);
    }

    public void addReferencedObject(Id centricId, String refField, ObjectId objectId) {
        referencedObjects.put(new ReferencedObjectKey(refField, centricId), objectId);
    }

    public Collection<ObjectId> getDereferencedObjects(Id centricId, String refField) {
        return dereferencedObjects.get(new ReferencedObjectKey(refField, centricId));
    }

    public Collection<ObjectId> getReferencedObjects(Id centricId, String refField) {
        return referencedObjects.get(new ReferencedObjectKey(refField, centricId));
    }

    private static class ReferencedObjectKey {

        private final String refField;
        private final Id centricId;

        public ReferencedObjectKey(String refField, Id centricId) {
            this.refField = refField;
            this.centricId = centricId;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 47 * hash + Objects.hashCode(this.refField);
            hash = 47 * hash + Objects.hashCode(this.centricId);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ReferencedObjectKey other = (ReferencedObjectKey) obj;
            if (!Objects.equals(this.refField, other.refField)) {
                return false;
            }
            if (!Objects.equals(this.centricId, other.centricId)) {
                return false;
            }
            return true;
        }
    }
}

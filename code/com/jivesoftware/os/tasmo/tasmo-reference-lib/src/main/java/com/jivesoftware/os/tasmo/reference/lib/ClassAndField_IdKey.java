/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.id.ObjectId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ClassAndField_IdKey implements Comparable<ClassAndField_IdKey> {

    private final String className;
    private final String fieldName;
    private final ObjectId objectId;

    public ClassAndField_IdKey(String className, String fieldName, ObjectId objectId) {
        checkNotNull(className, "className may not be null!");
        checkNotNull(fieldName, "fieldName may not be null!");
        checkNotNull(objectId, "objectId may not be null!");
        this.className = className;
        this.fieldName = fieldName;
        this.objectId = objectId;
    }

    public String getClassName() {
        return className;
    }

    public String getFieldName() {
        return fieldName;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    @Override
    public String toString() {
        return className + "." + fieldName + "." + objectId;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.className != null ? this.className.hashCode() : 0);
        hash = 29 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
        hash = 29 * hash + (this.objectId != null ? this.objectId.hashCode() : 0);
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
        final ClassAndField_IdKey other = (ClassAndField_IdKey) obj;
        if (this.className != other.className && (this.className == null || !this.className.equals(other.className))) {
            return false;
        }
        if (this.fieldName != other.fieldName && (this.fieldName == null || !this.fieldName.equals(other.fieldName))) {
            return false;
        }
        if (this.objectId != other.objectId && (this.objectId == null || !this.objectId.equals(other.objectId))) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ClassAndField_IdKey o) {
        int i = className.compareTo(o.className);
        if (i != 0) {
            return i;
        }
        i = fieldName.compareTo(o.fieldName);
        if (i != 0) {
            return i;
        }
        i = objectId.compareTo(o.objectId);
        if (i != 0) {
            return i;
        }
        return i;
    }
}

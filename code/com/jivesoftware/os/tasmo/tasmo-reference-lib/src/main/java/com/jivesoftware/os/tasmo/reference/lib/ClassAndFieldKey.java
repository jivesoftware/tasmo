/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ClassAndFieldKey implements Comparable<ClassAndFieldKey> {

    private final String className;
    private final String fieldName;

    public ClassAndFieldKey(String className, String fieldName) {
        checkNotNull(className);
        checkNotNull(fieldName);
        this.className = className;
        this.fieldName = fieldName;
    }

    public String getClassName() {
        return className;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return className + "." + fieldName;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + (this.className != null ? this.className.hashCode() : 0);
        hash = 41 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
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
        final ClassAndFieldKey other = (ClassAndFieldKey) obj;
        if (this.className != other.className && (this.className == null || !this.className.equals(other.className))) {
            return false;
        }
        if (this.fieldName != other.fieldName && (this.fieldName == null || !this.fieldName.equals(other.fieldName))) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ClassAndFieldKey o) {
        int i = className.compareTo(o.className);
        if (i != 0) {
            return i;
        }
        i = fieldName.compareTo(o.fieldName);
        if (i != 0) {
            return i;
        }
        return i;
    }
}

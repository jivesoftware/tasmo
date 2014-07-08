/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class TypedField {

    private final Set<String> fieldClass;
    private final String[] fieldNames;
    private final ValueType valueType;

    public TypedField(Set<String> fieldClass, String[] fieldNames, ValueType valueType) {
        this.fieldClass = fieldClass;
        this.fieldNames = fieldNames;
        this.valueType = valueType;
    }

    public Set<String> getFieldClasses() {
        return Collections.unmodifiableSet(fieldClass);
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public ValueType getValueType() {
        return valueType;
    }

    @Override
    public String toString() {
        return "TypedField{" + "fieldClass=" + fieldClass + ", fieldNames=" + fieldNames + ", valueType=" + valueType + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypedField that = (TypedField) o;

        if (!fieldClass.equals(that.fieldClass)) {
            return false;
        }
        if (!Arrays.equals(fieldNames, that.fieldNames)) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = fieldClass.hashCode();
        result = 31 * result + Arrays.hashCode(fieldNames);
        result = 31 * result + valueType.hashCode();
        return result;
    }
}

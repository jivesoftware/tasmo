package com.jivesoftware.os.tasmo.view.reader.api;

import com.jivesoftware.os.tasmo.event.api.ReservedFields;

public enum BackRefType {

    ALL(ReservedFields.ALL_BACK_REF_FIELD_PREFIX),
    LATEST(ReservedFields.LATEST_BACK_REF_FIELD_PREFIX),
    COUNT(ReservedFields.COUNT_BACK_REF_FIELD_PREFIX);

    private final String fieldNamePrefix;

    private BackRefType(String fieldNamePrefix) {
        this.fieldNamePrefix = fieldNamePrefix;
    }

    public String getFieldNamePrefix() {
        return fieldNamePrefix;
    }
}

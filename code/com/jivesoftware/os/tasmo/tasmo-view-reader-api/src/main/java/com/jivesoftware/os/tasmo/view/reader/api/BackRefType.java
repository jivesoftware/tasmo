package com.jivesoftware.os.tasmo.view.reader.api;

public enum BackRefType {

    ALL("all"),
    LATEST("latest");

    private final String fieldNamePrefix;

    private BackRefType(String fieldNamePrefix) {
        this.fieldNamePrefix = fieldNamePrefix;
    }

    public String getFieldNamePrefix() {
        return fieldNamePrefix;
    }
}

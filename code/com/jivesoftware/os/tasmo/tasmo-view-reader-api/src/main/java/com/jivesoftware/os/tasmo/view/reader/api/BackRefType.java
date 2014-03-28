package com.jivesoftware.os.tasmo.view.reader.api;

public enum BackRefType {

    ALL("all_"),
    LATEST("latest_"),
    COUNT("count_");

    private final String fieldNamePrefix;

    private BackRefType(String fieldNamePrefix) {
        this.fieldNamePrefix = fieldNamePrefix;
    }

    public String getFieldNamePrefix() {
        return fieldNamePrefix;
    }
}

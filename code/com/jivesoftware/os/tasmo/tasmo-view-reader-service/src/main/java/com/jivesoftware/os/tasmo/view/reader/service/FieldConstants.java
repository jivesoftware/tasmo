package com.jivesoftware.os.tasmo.view.reader.service;

public class FieldConstants {

    public static final String VIEW_OBJECT_ID = "objectId";
    /**
     * This is the system wide field name used for deleting anything via an event write.
     * It is implicitly bound to at the top level of any view, so that we can hide
     * views of things which have been deleted.
     */
    public static final String DELETED = "deleted";
    /**
     * This is the field in view json which holds the name of the view class
     */
    public static final String VIEW_CLASS = "viewClassName";
    /**
     * This is the field containing the tenant id which an event pertains to
     */
    public static final String TENANT_ID = "tenantId";
    /**
     * This prefix is prepended to any view field which represents a multi-back reference
     */
    public static final String ALL_BACK_REF_FIELD_PREFIX = "all_";
    /**
     * This prefix is prepended to any view field which represents a single back reference (latest referencer)
     */
    public static final String LATEST_BACK_REF_FIELD_PREFIX = "latest_";
}

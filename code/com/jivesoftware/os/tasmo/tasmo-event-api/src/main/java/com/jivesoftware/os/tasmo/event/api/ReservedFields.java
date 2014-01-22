/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.event.api;

/**
 * Defines constants for fields names which are used by the system both in
 * events and in tasmo view output. They cannot be used when declaring
 * events or when declaring view entries.
 */
public class ReservedFields {

    private ReservedFields() {
    }

    /**
     * The presence of this field means this events processing lifecycle should be tracked.
     */
    public static final String TRACK_EVENT_PROCESSED_LIFECYCLE = "trackEventProcessedLifecycle";

    /**
     * This is the field containing the model version that the event passed at time of ingress.
     */
    public static final String MODEL_VERSION_ID = "modelVersionId";

    /**
     * This is the field containing the snowflake of an event written to the system
     */
    public static final String EVENT_ID = "eventId";

    /**
     * This is the field containing the tenant id which an event pertains to
     */
    public static final String TENANT_ID = "tenantId";

    /**
     * This is the field containing the id of the user/actor generating an event this typically this is
     * the same as userId. However in cases where someone else is doing something on behalf of a given
     * userId the actorId will be different.
     */
    public static final String ACTOR_ID = "actorId";

    /**
     * This is the field containing the id of the user who is associated with the given event.
     */
    public static final String USER_ID = "userId";

    /**
     * This is the field containing the id of the event that caused the current event
     */
    public static final String CAUSED_BY = "causedBy";

    /**
     * This is field containing the snowflake portion of the object id of an
     * object being modified by an event
     */
    public static final String INSTANCE_ID = "instanceId";

    /**
     * This is the field in view json which holds the object id of some object
     * represented in the view. Every level of the view json will have this field
     */
    public static final String VIEW_OBJECT_ID = "objectId";

    /**
     * This is an optional field for the view json which holds the name of a
     * ref field in the view which should be marked as the root id of the view.
     */
    public static final String VIEW_ROOT_ID = "viewIdFieldName";

    /**
     * This is the field in view json which holds the name of the view class
     */
    public static final String VIEW_CLASS = "viewClassName";
    //the value of this constant need to match CommonEventConstants.DELETED up in the event.builder module
    /**
     * This is the system wide field name used for deleting anything via an event write.
     * It is implicitly bound to at the top level of any view, so that we can hide
     * views of things which have been deleted.
     */
    public static final String DELETED = "deleted";

    /**
     * This is a temporary field in an event denoting it's activity verb (to be used by the activity service)
     */
    public static final String ACTIVITY_VERB = "activityVerb";

    /**
     * This prefix is prepended to any view field which represents a multi-back reference
     */
    public static final String ALL_BACK_REF_FIELD_PREFIX = "all_";
    /**
     * This prefix is prepended to any view field which represents a single back reference (latest referencer)
     */
    public static final String LATEST_BACK_REF_FIELD_PREFIX = "latest_";

    public static int EVENT_FIELD_COUNT = 8;
}

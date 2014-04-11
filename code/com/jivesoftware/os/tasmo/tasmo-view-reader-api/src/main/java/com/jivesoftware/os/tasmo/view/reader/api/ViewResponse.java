package com.jivesoftware.os.tasmo.view.reader.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;

/**
 * View read response. Includes status of the operation and optionally the view body.
 */
public class ViewResponse {

    public enum StatusCode {

        OK,
        ERROR,
        NOT_FOUND,
        FORBIDDEN
    }
    private final StatusCode statusCode;
    private final ObjectNode viewBody;

    // JsonNode is the common superclass for ObjectNode and NullNode (which we get on deserialization if the body is null)
    public ViewResponse(@JsonProperty("statusCode") StatusCode statusCode,
        @JsonProperty("viewBody") JsonNode viewBody) {
        this.statusCode = statusCode;
        if (viewBody instanceof ObjectNode) {
            this.viewBody = (ObjectNode) viewBody;
        } else if (viewBody instanceof NullNode || viewBody == null) {
            this.viewBody = null;
        } else {
            throw new IllegalArgumentException("Unsupported type of viewBody - " + viewBody.getClass().getName());
        }
    }

    public static ViewResponse ok(ObjectNode viewBody) {
        return new ViewResponse(StatusCode.OK, viewBody);
    }

    public static ViewResponse error() {
        return new ViewResponse(StatusCode.ERROR, null);
    }

    public static ViewResponse forbidden() {
        return new ViewResponse(StatusCode.FORBIDDEN, null);
    }

    public static ViewResponse notFound() {
        return new ViewResponse(StatusCode.NOT_FOUND, null);
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    @Nullable
    public ObjectNode getViewBody() {
        return statusCode == StatusCode.OK ? viewBody : null;
    }

    public boolean hasViewBody() {
        return statusCode == StatusCode.OK && viewBody != null;
    }

    @Override
    public String toString() {
        return "ViewResponse{"
            + "statusCode=" + statusCode
            + ", viewBody=" + viewBody
            + '}';
    }
}

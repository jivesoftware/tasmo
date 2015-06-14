/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.api;

import com.jivesoftware.os.jive.utils.id.Id;
import java.util.Arrays;

public class ViewFieldVersion {
    private final Id[] pathIds;
    private final String fieldName;
    private final long timestamp;

    public ViewFieldVersion(Id[] pathIds, String fieldName, long timestamp) {
        this.pathIds = pathIds;
        this.fieldName = fieldName;
        this.timestamp = timestamp;
    }

    public Id[] getPathIds() {
        return pathIds;
    }

    public String getFieldName() {
        return fieldName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ViewValueFieldVersion{"
            + "pathIds=" + Arrays.toString(pathIds)
            + ", fieldName=" + fieldName
            + ", timestamp=" + timestamp
            + '}';
    }
}

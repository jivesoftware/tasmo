package com.jivesoftware.os.tasmo.lib.read;

import com.jivesoftware.os.tasmo.lib.write.ViewField;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class ViewFieldsResponse {

    private final List<ViewField> viewFields;
    private final Exception exception;

    public ViewFieldsResponse(List<ViewField> viewFields) {
        this.viewFields = viewFields;
        this.exception = null;
    }

    public ViewFieldsResponse(Exception exception) {
        this.viewFields = Collections.emptyList();
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

    public List<ViewField> getViewFields() {
        return viewFields;
    }

    public boolean isOk() {
        return exception == null;
    }

}

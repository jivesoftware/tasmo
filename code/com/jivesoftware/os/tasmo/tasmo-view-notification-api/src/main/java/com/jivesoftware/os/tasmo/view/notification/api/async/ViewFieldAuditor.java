package com.jivesoftware.os.tasmo.view.notification.api.async;

import com.jivesoftware.os.tasmo.id.BaseView;
import java.util.Collection;

/**
 *
 */
public interface ViewFieldAuditor<V extends BaseView<?>> extends ViewNotifiable<V> {

    Collection<?> auditFields(V auditable);
}

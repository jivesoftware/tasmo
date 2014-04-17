package com.jivesoftware.os.tasmo.test;

import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class AssertableCase {
    public final String category;
    public final long testId;
    public final Materialization materialization;
    public final TenantIdAndCentricId tenantIdAndCentricId;
    public final Id actorId;
    public final ViewBinding binding;
    public final EventWriterProvider eventWriterProvider;
    public final EventFire input;
    public final Set<Id> deletedId;

    public AssertableCase(String category,
            long testId,
            Materialization materialization,
            TenantIdAndCentricId tenantIdAndCentricId,
            Id actorId,
            ViewBinding binding,
            EventWriterProvider eventWriterProvider,
            EventFire input,
            Set<Id> deletedId) {
        this.category = category;
        this.testId = testId;
        this.materialization = materialization;
        this.tenantIdAndCentricId = tenantIdAndCentricId;
        this.actorId = actorId;
        this.binding = binding;
        this.eventWriterProvider = eventWriterProvider;
        this.input = input;
        this.deletedId = deletedId;
    }

}

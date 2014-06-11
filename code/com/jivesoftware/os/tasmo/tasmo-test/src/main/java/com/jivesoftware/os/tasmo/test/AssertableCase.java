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
    public String category;
    public long testId;
    public Materialization materialization;
    public TenantIdAndCentricId tenantIdAndCentricId;
    public Id actorId;
    public ViewBinding binding;
    public EventWriterProvider eventWriterProvider;
    public EventFire input;
    public Set<Id> deletedId;

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

    public void prepare(int numberOfEventProcessorThreads) throws Exception {
        materialization.setupModelAndMaterializer(numberOfEventProcessorThreads);
    }

    public void dispose() {
        materialization.shutdown();
        this.category = null;
        this.materialization = null;
        this.tenantIdAndCentricId = null;
        this.actorId = null;
        this.binding = null;
        this.eventWriterProvider = null;
        this.input = null;
        this.deletedId = null;
    }

}

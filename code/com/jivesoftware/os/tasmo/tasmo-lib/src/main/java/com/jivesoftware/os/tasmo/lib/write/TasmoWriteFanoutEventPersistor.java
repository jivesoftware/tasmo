package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.tasmo.lib.write.TasmoEventPersistor;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ExistenceUpdate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TasmoWriteFanoutEventPersistor implements TasmoEventPersistor {

    private final WrittenEventProvider writtenEventProvider;
    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ConcurrencyStore concurrencyStore;
    private final EventValueStore eventValueStore;
    private final ReferenceStore referenceStore;

    public TasmoWriteFanoutEventPersistor(WrittenEventProvider writtenEventProvider, WrittenInstanceHelper writtenInstanceHelper, ConcurrencyStore concurrencyStore,
        EventValueStore eventValueStore, ReferenceStore referenceStore) {
        this.writtenEventProvider = writtenEventProvider;
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.concurrencyStore = concurrencyStore;
        this.eventValueStore = eventValueStore;
        this.referenceStore = referenceStore;
    }

    @Override
    public void removeValueFields(VersionedTasmoViewModel model, String className, TenantIdAndCentricId tenantIdAndCentricId, ObjectId instanceId, long timestamp) {
        concurrencyStore.removeObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));

        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();
        Set<String> fieldNames = new HashSet<>();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            if (fieldNameAndType.getFieldType() == ModelPathStepType.value) {
                String fieldName = fieldNameAndType.getFieldName();
                fieldNames.add(fieldName);
            }
        }
        eventValueStore.removeObjectId(tenantIdAndCentricId, timestamp, instanceId, fieldNames.toArray(new String[fieldNames.size()]));
    }

    @Override
    public void updateValueFields(VersionedTasmoViewModel model, String className, TenantIdAndCentricId tenantIdAndCentricId, ObjectId instanceId, long timestamp, WrittenInstance writtenInstance) throws Exception {

        concurrencyStore.addObjectId(Arrays.asList(new ExistenceUpdate(tenantIdAndCentricId, timestamp, instanceId)));

        EventValueStore.Transaction transaction = eventValueStore.begin(tenantIdAndCentricId,
            timestamp,
            timestamp,
            instanceId);

        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel = model.getEventModel();

        List<String> refFieldNames = new ArrayList<>();
        for (TasmoViewModel.FieldNameAndType fieldNameAndType : eventModel.get(className)) {
            String fieldName = fieldNameAndType.getFieldName();
            if (writtenInstance.hasField(fieldName)) {
                if (fieldNameAndType.getFieldType() == ModelPathStepType.ref) {
                    refFieldNames.add(fieldName);
                } else {
                    OpaqueFieldValue got = writtenInstance.getFieldValue(fieldName);
                    if (got == null || got.isNull()) {
                        transaction.remove(fieldName);
                    } else {
                        transaction.set(fieldName, got);
                    }
                }
            }
        }

        // Always emit the nil field to signal presence
        OpaqueFieldValue nilValue = writtenEventProvider.createNilValue();
        transaction.set(ReservedFields.NIL_FIELD, nilValue);

        // 1 multiget
        List<Long> highests = concurrencyStore.highests(tenantIdAndCentricId, instanceId, refFieldNames.toArray(new String[refFieldNames.size()]));
        List<ReferenceStore.LinkTo> batchLinkTos = new ArrayList<>(refFieldNames.size());
        for (int i = 0; i < refFieldNames.size(); i++) {
            String fieldName = refFieldNames.get(i);
            if (highests == null || highests.get(i) == null || timestamp >= highests.get(i)) {
                // 4 multi puts
                OpaqueFieldValue fieldValue = writtenInstance.getFieldValue(fieldName);
                if (fieldValue.isNull()) {
                    batchLinkTos.add(new ReferenceStore.LinkTo(fieldName, Collections.<Reference>emptyList()));
                } else {
                    Collection<Reference> tos = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName);
                    batchLinkTos.add(new ReferenceStore.LinkTo(fieldName, tos));
                }
            }
        }
        if (!batchLinkTos.isEmpty()) {
            referenceStore.link(tenantIdAndCentricId, instanceId, timestamp, batchLinkTos);
        }
        // 3 to 6 multiputs
        eventValueStore.commit(transaction);
    }
}

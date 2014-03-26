/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.EventWrite;
import com.jivesoftware.os.tasmo.model.EventDefinition;
import com.jivesoftware.os.tasmo.model.EventFieldValueType;
import com.jivesoftware.os.tasmo.model.EventsModel;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;
import com.jivesoftware.os.tasmo.reference.lib.Reference;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import java.util.Collection;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class RefProcessor implements EventProcessor {

    private final WrittenInstanceHelper writtenInstanceHelper;
    private final ReferenceStore referenceStore;
    private final EventsModel eventsModel;

    public RefProcessor(WrittenInstanceHelper writtenInstanceHelper, ReferenceStore referenceStore, EventsModel eventsModel) {
        this.writtenInstanceHelper = writtenInstanceHelper;
        this.referenceStore = referenceStore;
        this.eventsModel = eventsModel;
    }

    @Override
    public boolean process(final EventWrite write) throws Exception {

        boolean wasProcessed = false;

        WrittenEvent writtenEvent = write.getWrittenEvent();
        TenantId tenantId = writtenEvent.getTenantId();
        long writtenOrderId = writtenEvent.getEventId();
        ObjectId objectId = writtenEvent.getWrittenInstance().getInstanceId();
        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        Reference objectInstanceId = new Reference(objectId, writtenOrderId);

        Id userId = writtenEvent.getCentricId();
        final TenantIdAndCentricId tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, userId);
        final TenantIdAndCentricId globalTenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        EventDefinition event = eventsModel.getEvent(writtenEvent.getWrittenInstance().getInstanceId().getClassName());



        if (writtenEvent.getWrittenInstance().isDeletion()) {
            for (Map.Entry<String, EventFieldValueType> entry : event.getEventFields().entrySet()) {
                final String refField = entry.getKey();
                //this handles things the deleted object references
                EventFieldValueType fieldType = entry.getValue();

                if (EventFieldValueType.ref.equals(fieldType) || EventFieldValueType.refs.equals(fieldType)) {

                    referenceStore.remove_aId_aField(tenantIdAndCentricId, writtenOrderId - 1, objectInstanceId, entry.getKey(),
                        new CallbackStream<Reference>() {
                        @Override
                        public Reference callback(Reference bId) throws Exception {
                            if (bId != null) {
                                write.addDereferencedObjects(tenantIdAndCentricId.getCentricId(), refField, bId.getObjectId());
                            }

                            return bId;
                        }
                    });

                    referenceStore.remove_aId_aField(globalTenantIdAndCentricId, writtenOrderId - 1, objectInstanceId, entry.getKey(),
                        new CallbackStream<Reference>() {
                        @Override
                        public Reference callback(Reference bId) throws Exception {
                            if (bId != null) {
                                write.addDereferencedObjects(globalTenantIdAndCentricId.getCentricId(), refField, bId.getObjectId());
                            }

                            return bId;
                        }
                    });
                }

                //to handle things which reference the deleted object, we'd need to have the model tell us all possible Aclass.Afield pairs could possibly
                //reference this object via an Aclass.Afield.deletedObjectId key. Then, scan each one to find aIds, and for whatever we found, do targeted
                //removals from the A->B mappinds, then remove
                //all the Aclass.Afield.aId -> bId cells. Then we need to delete all the Aclass.Afield.deletedObjectId keys where there were any values.

                //this is why we still want existence store to assert stuff doesn't exist.
            }
        } else {
            for (final String fieldName : writtenEvent.getWrittenInstance().getFieldNames()) {
                EventFieldValueType fieldType = event.getEventFields().get(fieldName);

                if (EventFieldValueType.ref.equals(fieldType) || EventFieldValueType.refs.equals(fieldType)) {
//                if (writtenEvent.getWrittenInstance().isDeletion()) {
//                    referenceStore.remove_aId_aField(tenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName,
//                        new CallbackStream<Reference>() {
//                        @Override
//                        public Reference callback(Reference bId) throws Exception {
//                            return null;
//                        }
//                    });
//                    referenceStore.remove_aId_aField(globalTenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName,
//                        new CallbackStream<Reference>() {
//                        @Override
//                        public Reference callback(Reference bId) throws Exception {
//                            return null;
//                        }
//                    });
//                } else {
//                    Collection<Reference> bIds = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName, writtenOrderId);
//                    if (bIds != null && !bIds.isEmpty()) {
//                        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName, bIds);
//                        referenceStore.link_aId_aField_to_bIds(globalTenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName, bIds);
//                        wasProcessed |= true;
//                    }
//                }
                    Collection<Reference> bIds = writtenInstanceHelper.getReferencesFromInstanceField(writtenInstance, fieldName, writtenOrderId);
                    if (bIds != null && !bIds.isEmpty()) {
                        referenceStore.link_aId_aField_to_bIds(tenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName, bIds);
                        referenceStore.link_aId_aField_to_bIds(globalTenantIdAndCentricId, writtenOrderId, objectInstanceId, fieldName, bIds);
                        wasProcessed |= true;
                    }

                    //always speculative remove - need to rework ref store to avoid this
                    referenceStore.remove_aId_aField(tenantIdAndCentricId, writtenOrderId - 1, objectInstanceId, fieldName,
                        new CallbackStream<Reference>() {
                        @Override
                        public Reference callback(Reference bId) throws Exception {
                            if (bId != null) {
                                write.addDereferencedObjects(tenantIdAndCentricId.getCentricId(), fieldName, bId.getObjectId());
                            }

                            return bId;
                        }
                    });

                    referenceStore.remove_aId_aField(globalTenantIdAndCentricId, writtenOrderId - 1, objectInstanceId, fieldName,
                        new CallbackStream<Reference>() {
                        @Override
                        public Reference callback(Reference bId) throws Exception {
                            if (bId != null) {
                                write.addDereferencedObjects(globalTenantIdAndCentricId.getCentricId(), fieldName, bId.getObjectId());
                            }

                            return bId;
                        }
                    });

                }
            }
        }

        return wasProcessed;
    }
}

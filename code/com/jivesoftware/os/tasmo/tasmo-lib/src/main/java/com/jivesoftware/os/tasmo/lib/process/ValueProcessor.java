/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.configuration.EventModel;
import com.jivesoftware.os.tasmo.configuration.EventsModel;
import com.jivesoftware.os.tasmo.configuration.ValueType;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore.Transaction;
import com.jivesoftware.os.tasmo.model.process.OpaqueFieldValue;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenInstance;

/**
 * @author jonathan.colt
 */
public class ValueProcessor implements EventProcessor {

    private final EventValueStore eventValueStore;
    private final EventsModel eventsModel;

    public ValueProcessor(EventValueStore eventValueStore, EventsModel eventsModel) {
        this.eventValueStore = eventValueStore;
        this.eventsModel = eventsModel;
    }

    @Override
    public boolean process(WrittenEvent writtenEvent) throws Exception {
        boolean wasProcessed = false;
        

        TenantId tenantId = writtenEvent.getTenantId();
        Id userId = writtenEvent.getCentricId();
        long writtenOrderId = writtenEvent.getEventId();

        WrittenInstance writtenInstance = writtenEvent.getWrittenInstance();
        ObjectId objectInstanceId = writtenInstance.getInstanceId();

        if (writtenInstance.isDeletion()) {
            eventValueStore.removeObjectId(new TenantIdAndCentricId(tenantId, userId), writtenOrderId, objectInstanceId);
            eventValueStore.removeObjectId(new TenantIdAndCentricId(tenantId, Id.NULL), writtenOrderId, objectInstanceId);
        } else {
            Transaction transaction = eventValueStore.begin(new TenantIdAndCentricId(tenantId, userId),
                writtenOrderId,
                writtenOrderId,
                objectInstanceId);
            Transaction globalTransaction = eventValueStore.begin(new TenantIdAndCentricId(tenantId, Id.NULL),
                writtenOrderId,
                writtenOrderId,
                objectInstanceId);
            
            for (String fieldName : writtenEvent.getWrittenInstance().getFieldNames()) {
                OpaqueFieldValue got = writtenInstance.getFieldValue(fieldName);
                if (got == null || got.isNull()) {
                    transaction.remove(fieldName);
                    globalTransaction.remove(fieldName);
                } else {
                    transaction.set(fieldName, got);
                    globalTransaction.set(fieldName, got);
                }
            }
          
            eventValueStore.commit(transaction);
            eventValueStore.commit(globalTransaction);
        }

        return wasProcessed;
    }
}

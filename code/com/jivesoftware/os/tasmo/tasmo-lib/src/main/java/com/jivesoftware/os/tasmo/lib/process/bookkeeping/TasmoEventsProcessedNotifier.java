/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process.bookkeeping;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.id.Id;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;

public class TasmoEventsProcessedNotifier {

    private final CallbackStream<List<BookkeepingEvent>> eventProcessedNotifier;

    public TasmoEventsProcessedNotifier(CallbackStream<List<BookkeepingEvent>> eventProcessedNotifier) {
        this.eventProcessedNotifier = eventProcessedNotifier;
    }

    public void notify(List<WrittenEvent> successful, List<WrittenEvent> failed) throws Exception {
        List<BookkeepingEvent> notifications = new ArrayList<>();
        notifications.addAll(transformToBookkeepingEvents(successful, true));
        notifications.addAll(transformToBookkeepingEvents(failed, false));
        if (!notifications.isEmpty()) {
            eventProcessedNotifier.callback(notifications);
        }
    }

    private List<BookkeepingEvent> transformToBookkeepingEvents(List<WrittenEvent> writtenEvents, boolean successful) {
        List<BookkeepingEvent> notifications = new ArrayList<>();
        for (WrittenEvent writtenEvent : writtenEvents) {
            if (writtenEvent == null) {
                continue;
            }
            TenantId tenantId = writtenEvent.getTenantId();
            Id actorId = writtenEvent.getActorId();
            long eventId = writtenEvent.getEventId();
            Boolean isBookeepingEnabled = writtenEvent.isBookKeepingEnabled();
            if (isBookeepingEnabled) {
                notifications.add(new BookkeepingEvent(tenantId, actorId, eventId, successful));
            }
        }
        return notifications;
    }
}

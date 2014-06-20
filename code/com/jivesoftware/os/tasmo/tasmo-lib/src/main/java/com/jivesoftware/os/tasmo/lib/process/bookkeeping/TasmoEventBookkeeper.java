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
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;

public class TasmoEventBookkeeper {

    private final CallbackStream<List<BookkeepingEvent>> bookKeepingStream;
    private final ThreadLocal<Session> bookkeepingSession;

    public TasmoEventBookkeeper(CallbackStream<List<BookkeepingEvent>> bookKeepingStream) {
        this.bookKeepingStream = bookKeepingStream;
        this.bookkeepingSession = new ThreadLocal<Session>() {
            @Override
            protected Session initialValue() {
                return new Session();
            }
        };
    }

    public void begin(List<WrittenEvent> writtenEvents) throws Exception {

        Session session = bookkeepingSession.get();
        List<BookkeepingEvent> initialStatus = new ArrayList<>();

        for (WrittenEvent writtenEvent : writtenEvents) {
            if (writtenEvent == null) {
                continue;
            }
            TenantId tenantId = writtenEvent.getTenantId();
            Id actorId = writtenEvent.getActorId();
            long eventId = writtenEvent.getEventId();
            Boolean isBookeepingEnabled = writtenEvent.isBookKeepingEnabled();
            if (isBookeepingEnabled) {
                initialStatus.add(new BookkeepingEvent(tenantId, actorId, eventId, true));
            }
        }


        session.initialize(initialStatus);
    }

    public void failed() throws Exception {
        List<BookkeepingEvent> failures = bookkeepingSession.get().getFailedStatus();
        if (!failures.isEmpty()) {
            bookKeepingStream.callback(failures);
        }
        bookkeepingSession.remove();
    }

    public void succeeded() throws Exception {
        List<BookkeepingEvent> successes = bookkeepingSession.get().getSuccessStatus();
        if (!successes.isEmpty()) {
            bookKeepingStream.callback(successes);
        }
        bookkeepingSession.remove();
    }

    private static class Session {

        private List<BookkeepingEvent> status;

        private void initialize(List<BookkeepingEvent> initialStatus) {
            this.status = initialStatus;
        }

        private List<BookkeepingEvent> getFailedStatus() {
            List<BookkeepingEvent> failedStatus = new ArrayList<>();

            for (BookkeepingEvent existing : status) {
                failedStatus.add(new BookkeepingEvent(
                        existing.getTenantId(), existing.getActorId(), existing.getEventId(), false));
            }

            return failedStatus;
        }

        private List<BookkeepingEvent> getSuccessStatus() {
            return status;
        }
    }
}

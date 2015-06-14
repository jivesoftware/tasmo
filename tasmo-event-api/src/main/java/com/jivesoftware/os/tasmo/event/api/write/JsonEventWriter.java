/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.event.api.write;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 *
 */
public interface JsonEventWriter {

    /**
     * Sends the provided events to the write-ahead log (WAL). Successful return guarantees that all events have been written, but failure is not transactional,
     * i.e. some or all events may have been sent even if an exception was thrown.
     *
     * @param events the list of events (JSON) to send. Individual events are assumed to be well formed and pass
     * @param synchronous set true for synchronous delivery, false for asynchronous
     * @param giveUpAfterNMillis only applicable when synchronous is true.
     *
     * {@link com.jivesoftware.jive.platform.events.event.api.JsonEventConventions#validate(com.fasterxml.jackson.databind.node.ObjectNode)}
     * @return an EventWriterResponse which contains the list of object ids, and event ids for the events that were sent. The order corresponds to the list of
     * events that were passed in. Successful return indicates that all events were acknowledged by the WAL.
     *
     * @throws JsonEventWriteException if the event write failed. Note that this is method is not transactional, i.e. some or all events may have been sent even
     * if an exception was thrown.
     */
    public EventWriterResponse write(List<ObjectNode> events, EventWriterOptions options) throws JsonEventWriteException;
}

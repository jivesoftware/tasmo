/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.service;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.lib.EventWrite;
import com.jivesoftware.os.tasmo.lib.TasmoViewMaterializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class EventIngressCallbackStream implements CallbackStream<List<WrittenEvent>> {

    final TasmoViewMaterializer materializer;
    final CallbackStream<List<EventWrite>> forkedOutput;

    public EventIngressCallbackStream(TasmoViewMaterializer materializer) {
        this(materializer, null);
    }

    public EventIngressCallbackStream(TasmoViewMaterializer materializer, CallbackStream<List<EventWrite>> forkedOutput) {
        this.materializer = materializer;
        this.forkedOutput = forkedOutput;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> value) throws Exception {
        if (value != null) {
            List<EventWrite> eventBatch = new ArrayList<>();
            for (WrittenEvent writtenEvent : value) {
                eventBatch.add(new EventWrite(writtenEvent));
            }
            materializer.process(eventBatch);
            forkedOutput.callback(eventBatch);
        }


        return value;
    }
}

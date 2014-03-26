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
import com.jivesoftware.os.tasmo.lib.TasmoViewMaterializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.List;

/**
 *
 */
public class EventIngressCallbackStream implements CallbackStream<List<WrittenEvent>> {

    final TasmoViewMaterializer materializer;
    final CallbackStream<List<WrittenEvent>> forkedOutput;

    public EventIngressCallbackStream(TasmoViewMaterializer materializer) {
        this(materializer, null);
    }
    
    public EventIngressCallbackStream(TasmoViewMaterializer materializer, CallbackStream<List<WrittenEvent>> forkedOutput) {
        this.materializer = materializer;
        this.forkedOutput = forkedOutput;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> value) throws Exception {
        if (value != null) {
            materializer.process(value);
            forkedOutput.callback(value);
        }
        

        return value;
    }
}

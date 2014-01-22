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

    public EventIngressCallbackStream(TasmoViewMaterializer materializer) {
        this.materializer = materializer;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> value) throws Exception {
        if (value != null) {
            materializer.process(value);
        }

        return value;
    }
}

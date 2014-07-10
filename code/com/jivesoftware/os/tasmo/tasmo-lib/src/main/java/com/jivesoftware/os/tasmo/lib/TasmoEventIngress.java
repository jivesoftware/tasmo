/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.tasmo.lib.write.TasmoWriteMaterializer;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.List;

/**
 *
 */
public class TasmoEventIngress implements CallbackStream<List<WrittenEvent>> {

    final TasmoWriteMaterializer materializer;

    public TasmoEventIngress(TasmoWriteMaterializer materializer) {
        this.materializer = materializer;
    }

    @Override
    public List<WrittenEvent> callback(List<WrittenEvent> value) throws Exception {
        if (value != null) {
            return materializer.process(value);
        }
        return value;
    }
}

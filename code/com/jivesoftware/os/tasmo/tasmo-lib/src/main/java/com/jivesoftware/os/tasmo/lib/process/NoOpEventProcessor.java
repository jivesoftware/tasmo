/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

/**
 *
 * @author jonathan.colt
 */
public class NoOpEventProcessor implements EventProcessor {

    @Override
    public boolean process(WrittenEvent writtenEvent) throws Exception {
        return false;
    }
}

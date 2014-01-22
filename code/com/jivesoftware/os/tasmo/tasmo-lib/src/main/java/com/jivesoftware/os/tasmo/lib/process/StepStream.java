/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.reference.lib.Reference;

/**
 *
 * @author jonathan.colt
 */
public interface StepStream {

    void stream(Reference reference) throws Exception; // TODO: Consider batching?

    int getStepIndex();
}

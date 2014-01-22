/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib.write;

/**
 *
 */
public class CommitChangeException extends Exception {

    public CommitChangeException(String message) {
        super(message);
    }

    public CommitChangeException(String message, Throwable cause) {
        super(message, cause);
    }
}

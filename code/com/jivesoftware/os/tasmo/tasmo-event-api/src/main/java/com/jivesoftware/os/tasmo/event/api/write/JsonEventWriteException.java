/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.event.api.write;

/**
 *
 */
public class JsonEventWriteException extends Exception {

    public JsonEventWriteException() {
    }

    public JsonEventWriteException(String message) {
        super(message);
    }

    public JsonEventWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.jivesoftware.os.tasmo.event.api.write;

/**
 *
 *
 */
public class EventWriteException extends Exception {
    public EventWriteException() {
    }

    public EventWriteException(String message) {
        super(message);
    }

    public EventWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventWriteException(Throwable cause) {
        super(cause);
    }

    public EventWriteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

package com.jivesoftware.os.tasmo.event.api.write;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the various modes in which event emission can be requested.
 */
public class EventWriterOptions {

    private static final EventWriterOptions DEFAULT = EventWriterOptions.builder().build();

    /**
     * The default period of time to wait for a synchronous write to complete.
     */
    public static final int DEFAULT_WAIT_TIMEOUT_MILLIS = 15 * 1_000;

    private final boolean inFlightTracking;
    private final boolean synchronous;
    private final int syncronousTimeout;

    private EventWriterOptions(boolean inFlightTracking, boolean synchronous, int syncronousTimeout) {
        this.inFlightTracking = inFlightTracking;
        this.synchronous = synchronous;
        this.syncronousTimeout = syncronousTimeout;
    }

    /**
     * The default, most performant mode of operation.  Currently:  no tracking, asynchronous.
     *
     * @return default options
     */
    public static EventWriterOptions defaultOptions() {
        return DEFAULT;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        boolean inFlightTracking;
        int syncronousTimeout = -1;

        public Builder synchronous() {
            return synchronous(DEFAULT_WAIT_TIMEOUT_MILLIS);
        }

        public Builder synchronous(int giveUpAfterNMillis) {
            Preconditions.checkArgument(giveUpAfterNMillis > 0L, "giveUpAfterNMillis must be > 0");
            syncronousTimeout = giveUpAfterNMillis;

            return tracked();
        }

        public Builder tracked() {
            this.inFlightTracking = true;
            return this;
        }

        public EventWriterOptions build() {
            return new EventWriterOptions(inFlightTracking, syncronousTimeout > 0, syncronousTimeout);
        }
    }

    /**
     * Whether events should be tracked when written, allowing observation of when
     * they become eventually consistent.
     *
     * @return {@code true} if tracking is enabled, {@code false otherwise}
     */
    public boolean isInFlightTrackingEnabled() {
        return inFlightTracking;
    }

    /**
     * Whether or not the events should be emitted synchronously.
     *
     * @return {@code true} if they should, {@false otherwise}
     */
    public boolean isSynchronous() {
        return synchronous;
    }

    /**
     * If emitting synchronously, the maximum amount of time to wait, in milliseconds.
     *
     * @return millis to wait, or {@code -1} if emitting asynchronously
     */
    public int getSyncronousTimeoutInMillis() {
        return syncronousTimeout;
    }

}

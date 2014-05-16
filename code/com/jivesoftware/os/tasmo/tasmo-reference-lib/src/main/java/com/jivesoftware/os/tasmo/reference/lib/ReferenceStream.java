package com.jivesoftware.os.tasmo.reference.lib;

/**
 *
 * @author jonathan
 */
public interface ReferenceStream {

    ReferenceWithTimestamp stream(ReferenceWithTimestamp value) throws Exception;

    void failure(Exception cause);
}

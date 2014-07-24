package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;

/**
 *
 */
public class SaltingImmutableByteArrayMarshaller
        extends SaltingDelegatingMarshaller<ImmutableByteArrayMarshaller, ImmutableByteArray> {

    public SaltingImmutableByteArrayMarshaller() {
        super(new ImmutableByteArrayMarshaller());
    }
}

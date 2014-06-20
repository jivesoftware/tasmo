/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;


/**
 *
 */
public class ImmutableByteArrayMarshaller implements TypeMarshaller<ImmutableByteArray> {

    public ImmutableByteArrayMarshaller() {
    }

    @Override
    public ImmutableByteArray fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ImmutableByteArray t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ImmutableByteArray fromLexBytes(byte[] lexBytes) throws Exception {
        return new ImmutableByteArray(lexBytes);
    }

    @Override
    public byte[] toLexBytes(ImmutableByteArray t) throws Exception {
        return t.getImmutableBytes();
    }
}

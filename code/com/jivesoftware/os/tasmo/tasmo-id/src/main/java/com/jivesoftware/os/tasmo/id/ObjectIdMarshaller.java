/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ObjectIdMarshaller implements TypeMarshaller<ObjectId> {

    public ObjectIdMarshaller() {
    }

    @Override
    public ObjectId fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ObjectId t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ObjectId fromLexBytes(byte[] lexBytes) throws Exception {
        checkNotNull(lexBytes);
        return new ObjectId(ByteBuffer.wrap(lexBytes));
    }

    @Override
    public byte[] toLexBytes(ObjectId objectId) throws Exception {
        checkNotNull(objectId);
        return objectId.toLexBytes().array();
    }
}

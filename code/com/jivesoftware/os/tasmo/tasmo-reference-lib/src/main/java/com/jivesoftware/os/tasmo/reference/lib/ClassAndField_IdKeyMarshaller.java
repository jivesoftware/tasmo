/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.reference.lib;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import com.jivesoftware.os.tasmo.id.ObjectId;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ClassAndField_IdKeyMarshaller implements TypeMarshaller<ClassAndField_IdKey> {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    public ClassAndField_IdKeyMarshaller() {
    }

    @Override
    public ClassAndField_IdKey fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ClassAndField_IdKey t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ClassAndField_IdKey fromLexBytes(byte[] lexBytes) throws Exception {
        checkNotNull(lexBytes);
        ByteBuffer bb = ByteBuffer.wrap(lexBytes);

        int length = bb.getInt();
        checkLength(bb, length);
        byte[] raw = new byte[length];
        bb.get(raw);
        String className = new String(raw, UTF8);

        length = bb.getInt();
        checkLength(bb, length);
        raw = new byte[length];
        bb.get(raw);
        String fieldName = new String(raw, UTF8);
        ObjectId objectId = new ObjectId(bb);

        return new ClassAndField_IdKey(className, fieldName, objectId);
    }

    private void checkLength(ByteBuffer bb, int length) {
        if (length < 1) {
            throw new IndexOutOfBoundsException();
        }

        if (bb.remaining() < length) {
            throw new BufferUnderflowException();
        }
    }

    @Override
    public byte[] toLexBytes(ClassAndField_IdKey key) throws Exception {
        checkNotNull(key);
        byte[] classNameBytes = key.getClassName().getBytes(UTF8);
        byte[] fieldNameBytes = key.getFieldName().getBytes(UTF8);
        byte[] objectIdBytes = key.getObjectId().toLexBytes().array();
        ByteBuffer bb = ByteBuffer.allocate(4 + classNameBytes.length + 4 + fieldNameBytes.length + objectIdBytes.length);
        bb.putInt(classNameBytes.length);
        bb.put(classNameBytes);
        bb.putInt(fieldNameBytes.length);
        bb.put(fieldNameBytes);
        bb.put(objectIdBytes);
        return bb.array();
    }
}

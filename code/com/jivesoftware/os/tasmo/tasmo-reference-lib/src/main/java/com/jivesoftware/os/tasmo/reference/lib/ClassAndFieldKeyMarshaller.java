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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class ClassAndFieldKeyMarshaller implements TypeMarshaller<ClassAndFieldKey> {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    public ClassAndFieldKeyMarshaller() {
    }

    @Override
    public ClassAndFieldKey fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ClassAndFieldKey t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ClassAndFieldKey fromLexBytes(byte[] lexBytes) throws Exception {
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

        return new ClassAndFieldKey(className, fieldName);
    }

    @Override
    public byte[] toLexBytes(ClassAndFieldKey key) throws Exception {
        checkNotNull(key);
        byte[] classNameBytes = key.getClassName().getBytes(UTF8);
        byte[] fieldNameBytes = key.getFieldName().getBytes(UTF8);
        ByteBuffer bb = ByteBuffer.allocate(4 + classNameBytes.length + 4 + fieldNameBytes.length);
        bb.putInt(classNameBytes.length);
        bb.put(classNameBytes);
        bb.putInt(fieldNameBytes.length);
        bb.put(fieldNameBytes);
        return bb.array();
    }

    private void checkLength(ByteBuffer bb, int length) {
        if (length < 1) {
            throw new IndexOutOfBoundsException();
        }

        if (bb.remaining() < length) {
            throw new BufferUnderflowException();
        }
    }
}

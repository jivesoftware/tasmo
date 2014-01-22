/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class ObjectIdTest {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    public void testFromString() {
        String stringId = new Id(1).toStringForm();
        assertEquals(new ObjectId("foo_" + stringId), new ObjectId("foo", new Id(1)));
    }

    @Test
    public void testFromByteBuffer() {
        byte[] idBytes = longBytes(1);
        ByteBuffer bb = ByteBuffer.allocate(1 + idBytes.length + 4 + "foo".length());
        bb.put((byte) idBytes.length);
        bb.put(idBytes);
        byte[] bytes = "foo".getBytes(UTF8);
        bb.putInt(bytes.length);
        bb.put(bytes);

        assertEquals(new ObjectId(ByteBuffer.wrap(bb.array())), new ObjectId("foo", new Id(1)));
    }

    @Test
    public void testFromDataIn() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        byte[] idBytes = longBytes(1);
        dataOutput.write((byte) idBytes.length);
        dataOutput.write(idBytes);
        dataOutput.writeInt("foo".length());
        dataOutput.write("foo".getBytes(UTF8));

        DataInput dataInput = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        assertEquals(new ObjectId(dataInput), new ObjectId("foo", new Id(1)));
    }

    private byte[] longBytes(long _v) {
        return longBytes(_v, new byte[8], 0);
    }

    private byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }
}

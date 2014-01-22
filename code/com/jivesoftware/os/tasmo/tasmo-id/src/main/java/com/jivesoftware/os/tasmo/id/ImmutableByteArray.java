/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.Charset;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Exists largely so we can use byte arrays as keys in maps and sets
 */
public class ImmutableByteArray implements Comparable {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    /**
     *
     */
    public static final ImmutableByteArray NULL = new ImmutableByteArray(new byte[0]);
    private int hashCode = 0;
    private String toString = null;
    private byte[] bytes;

    /**
     *
     * @param bytes
     */
    @JsonCreator
    public ImmutableByteArray(
        @JsonProperty ("bytes") byte[] bytes) {
        // this should make a copy to make ImmutableByteArray truly Immutable
        // I have deliberately chosen not to for performance reasons.
        this.bytes = bytes;
    }

    /**
     *
     * @param string
     */
    public ImmutableByteArray(String string) {
        this(string.getBytes(UTF8));
    }

    /**
     *
     * @return
     */
    @JsonProperty ("bytes")
    public byte[] getImmutableBytes() {
        // this should return a copy to make ImmutableByteArray truly Immutable
        // I have deliberately chosen not to for performance reasons.
        return bytes;
    }

    @Override
    public String toString() {
        if (bytes == null) {
            return "";
        } else if (toString == null) {
            toString = new String(bytes, UTF8);
        }

        return toString;
    }

    /**
     *
     * @return
     */
    public int length() {
        if (bytes == null) {
            return -1;
        }
        return bytes.length;
    }

    @Override
    public int hashCode() {
        if ((bytes == null) || (bytes.length == 0)) {
            return 0;
        }

        if (hashCode != 0) {
            return hashCode;
        }

        int hash = 0;
        long randMult = 0x5DEECE66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = bytes.length;

        for (int i = 0; i < bytes.length; i++) {
            long x = (seed * randMult + randAdd) & randMask;

            seed = x;
            hash += (bytes[i] + 128) * x;
        }

        hashCode = hash;

        return hash;
    }

    @Override
    public boolean equals(Object _object) {
        if (_object == this) {
            return true;
        }
        byte[] b = null;
        if (_object instanceof byte[]) {
            b = (byte[]) _object;
        } else if (_object instanceof ImmutableByteArray) {
            b = ((ImmutableByteArray) _object).bytes;
        }
        if (b == null) {
            return false;
        }
        byte[] a = bytes;
        return Arrays.equals(a, b);
    }

    @Override
    public int compareTo(Object o) {
        checkNotNull(o);
        byte[] b;
        if (o instanceof byte[]) {
            b = (byte[]) o;
        } else if (o instanceof ImmutableByteArray) {
            b = ((ImmutableByteArray) o).bytes;
        } else {
            b = new byte[0];
        }
        if (b.length < bytes.length) {
            return 1;
        } else if (b.length > bytes.length) {
            return -1;
        } else {
            for (int i = 0; i < bytes.length; i++) {
                if (b[i] < bytes[i]) {
                    return 1;
                } else if (b[i] > bytes[i]) {
                    return -1;
                }
            }
            return 0;
        }
    }

    public boolean startsWith(byte[] bs) {
        int l = Math.min(bytes.length, bs.length);
        for (int i = 0; i < l; i++) {
            if (bs[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}

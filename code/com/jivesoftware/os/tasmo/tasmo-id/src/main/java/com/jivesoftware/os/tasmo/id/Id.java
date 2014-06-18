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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.io.BaseEncoding;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Id implements Comparable<Id> {

    public static final Id NULL = new Id(new byte[]{0});
    private static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    private final byte[] id;

    @JsonCreator
    public static Id idFromJsonObject(@JsonProperty("id") String stringForm) {
        byte [] id = coder.decode(stringForm);
        return (id.length == 8) ? new Id(id) : new Id(stringForm, true);
    }

    public Id(long id) {
        if (id < 0) {
            throw new IllegalArgumentException("negative ids are not supported. id:" + id);
        }
        this.id = ByteBuffer.allocate(8).putLong(id).array();
    }

    public Id(byte[] id) {
        this.id = id;
    }

    @Deprecated
    public Id(String stringForm) {
        if (stringForm == null || stringForm.length() == 0) {
            throw new IllegalArgumentException("stringForm can not be null and must be at least 1 or more chars." + stringForm);
        }

        if(NULL.toStringForm().equals(stringForm)) {
            this.id = coder.decode(stringForm);
        } else {
            byte[] decodedString = coder.decode(stringForm);
            if (decodedString.length < 8) decodedString = Arrays.copyOf(decodedString, 8);
            ByteBuffer buffer = ByteBuffer.wrap(decodedString);
            this.id = ByteBuffer.allocate(8).putLong(buffer.getLong()).array();
        }
    }

    protected Id(String stringForm, boolean legacy) {
        if (!legacy) {
            throw new IllegalArgumentException("this private constructor is for legacy purposes only");
        }
        if (stringForm == null || stringForm.length() == 0) {
            throw new IllegalArgumentException("stringForm can not be null and must be at least 1 or more chars." + stringForm);
        }
        this.id = coder.decode(stringForm);
    }

    public boolean isNull() {
        return this == NULL || equals(NULL);
    }

    public byte[] toBytes() {
        return Arrays.copyOf(id, id.length);
    }

    public byte[] toLengthPrefixedBytes() {
        byte[] bytes = new byte[id.length + 1];
        System.arraycopy(id, 0, bytes, 1, id.length);
        bytes[0] = (byte) id.length;
        return bytes;
    }

    public String getId() {
        return coder.encode(id);
    }

    @JsonValue
    public String toStringForm() {
        return coder.encode(id);
    }

    @Override
    public String toString() {
        return "Id{" + "id=" + coder.encode(id) + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 73 * hash + Arrays.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Id other = (Id) obj;
        if (!Arrays.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Id o) {
        return compare(id, o.id);
    }

    /**
     * Comparator for byte array (lexicographic) borrowed from Apache HBase
     *
     * @param left
     * @param right
     * @return
     */
    private int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
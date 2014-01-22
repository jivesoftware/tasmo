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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.DataInput;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Uniquely identifies an object in the system. They encompass the type of the object being identified, so object ids of different types will not collide.
 *
 * ObjectIds can be sorted by using the result of toLexBytes.
 */
public class ObjectId implements Comparable<ObjectId> {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final String className;
    private final Id id;

    @JsonCreator
    public ObjectId(String stringForm) {
        Preconditions.checkNotNull(stringForm);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stringForm.trim()), "String is empty!");
        String[] className_id = stringForm.split("_");
        if (className_id.length != 2) {
            throw new IllegalArgumentException("Expected 2 fields in input:" + stringForm + " but found " + className_id.length);
        }

        checkClassname(className_id[0]);

        this.className = className_id[0];
        this.id = new Id(className_id[1]);
    }

    @JsonCreator
    public ObjectId(
        @JsonProperty ("className") String className,
        @JsonProperty ("id") Id id) {
        Preconditions.checkNotNull(className);
        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(className.trim()), "Class name must be non-empty");

        checkClassname(className);

        this.className = className;
        this.id = id;
    }

    private void checkClassname(String className) {
        for (char c : className.toCharArray()) {
            if (!Character.isLetterOrDigit(c)) {
                throw new IllegalArgumentException("Classname " + className + " contains non alphanumeric characters.");
            }
        }
    }

    public static boolean isStringForm(String stringForm) {
        try {
            return isValidate(stringForm);
        } catch (Exception ex) {
            return false;
        }
    }

    private static boolean isValidate(String stringForm) {
        Preconditions.checkNotNull(stringForm);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stringForm.trim()), "String is empty!");
        String[] className_id = stringForm.split("_");
        if (className_id.length != 2) {
            throw new IllegalArgumentException("Expected 2 fields in input:" + stringForm + " but found " + className_id.length);
        }

        if (className_id[0].length() == 0) {
            throw new IllegalArgumentException("className length must be greater than 0.");
        }
        try {
            new Id(className_id[1]);
        } catch (Exception numberFormatException) {
            throw new IllegalArgumentException("id is not a valid long.");
        }
        return true;
    }

    public String toStringForm() {
        return className + "_" + id.toStringForm();
    }

    public String getClassName() {
        return className;
    }

    public Id getId() {
        return id;
    }

    public boolean isClassName(String className) {
        return this.className.equals(className);
    }

    public ObjectId(ByteBuffer bb) {
        int idLength = (int) bb.get();
        byte[] idBytes = new byte[idLength];
        bb.get(idBytes);
        this.id = new Id(idBytes);

        int length = bb.getInt();
        checkLength(bb, length);
        byte[] rawClassName = new byte[length];
        bb.get(rawClassName);
        this.className = new String(rawClassName, UTF8);
    }

    public ObjectId(DataInput input) throws IOException {
        int idLength = (int) input.readByte();
        byte[] idBytes = new byte[idLength];
        input.readFully(idBytes);
        this.id = new Id(idBytes);
        int length = input.readInt();
        if (length < 1 && length > 1024 * 256) {
            throw new IndexOutOfBoundsException();
        }
        byte[] rawClassName = new byte[length];
        input.readFully(rawClassName);
        this.className = new String(rawClassName, UTF8);
    }

    private void checkLength(ByteBuffer bb, int length) {
        if (length < 1) {
            throw new IndexOutOfBoundsException();
        }

        if (bb.remaining() < length) {
            throw new BufferUnderflowException();
        }
    }

    public ByteBuffer toLexBytes() {
        byte[] bytes = className.getBytes(UTF8);
        byte[] idBytes = id.toBytes();
        ByteBuffer bb = ByteBuffer.allocate(1 + idBytes.length + 4 + bytes.length);
        bb.put((byte) idBytes.length);
        bb.put(idBytes);
        bb.putInt(bytes.length);
        bb.put(bytes);
        return bb;
    }

    @Override
    public String toString() {
        return className + "_" + id.toStringForm();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.className != null ? this.className.hashCode() : 0);
        hash = 97 * hash + (this.id != null ? this.id.hashCode() : 0);
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
        final ObjectId other = (ObjectId) obj;
        if (this.className != other.className && (this.className == null || !this.className.equals(other.className))) {
            return false;
        }
        if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ObjectId o) {
        int i = id.compareTo(o.id);
        if (i != 0) {
            return i;
        }
        i = className.compareTo(o.className); //not a stable order at this point
        return i;
    }
}

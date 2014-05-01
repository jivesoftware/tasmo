package com.jivesoftware.os.tasmo.model.process;

import java.io.IOException;

/**
 * Represents a map of literal field values at the tail of a model path.
  *
 */
public interface LeafNodeFields {

    public void addField(String fieldName, OpaqueFieldValue value);

    public void removeField(String fieldName);

    public OpaqueFieldValue getField(String fieldName);

    public boolean hasField(String fieldName);

    public byte[] toBytes() throws IOException;

    public boolean hasFields();

    public void addBooleanField(String fieldName, boolean value);

    public Boolean getBooleanField(String fieldname);
}

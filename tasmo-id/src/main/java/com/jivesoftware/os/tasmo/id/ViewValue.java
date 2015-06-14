package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

/**
 *
 * @author jonathan
 */
public class ViewValue {

    private final long[] modelPathTimeStamps;
    private final byte[] value;

    @JsonCreator
    public ViewValue(@JsonProperty("modelPathTimeStamps") long[] modelPathTimeStamps,
            @JsonProperty("value") byte[] value) {
        this.modelPathTimeStamps = modelPathTimeStamps;
        this.value = value;
    }

    public long[] getModelPathTimeStamps() {
        return modelPathTimeStamps;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ViewValue{" + "modelPathTimeStamps=" + Arrays.toString(modelPathTimeStamps) + ", value=" + new String(value) + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + Arrays.hashCode(this.modelPathTimeStamps);
        hash = 67 * hash + Arrays.hashCode(this.value);
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
        final ViewValue other = (ViewValue) obj;
        if (!Arrays.equals(this.modelPathTimeStamps, other.modelPathTimeStamps)) {
            return false;
        }
        if (!Arrays.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }

}

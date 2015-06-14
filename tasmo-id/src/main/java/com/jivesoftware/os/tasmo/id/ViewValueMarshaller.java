package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan
 */
public class ViewValueMarshaller implements TypeMarshaller<ViewValue> {

    public ViewValueMarshaller() {
    }

    @Override
    public ViewValue fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ViewValue t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ViewValue fromLexBytes(byte[] lexBytes) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(lexBytes);
        byte version = bb.get();
        if (version == 0) {
            int length = bb.get();
            long[] modePathTimestamp = new long[length];
            for (int i = 0; i < length; i++) {
                modePathTimestamp[i] = bb.getLong();
            }
            byte[] value = new byte[bb.remaining()];
            bb.get(value);
            return new ViewValue(modePathTimestamp, value);
        }
        throw new RuntimeException("UnsupportedVersion:" + version);
    }

    @Override
    public byte[] toLexBytes(ViewValue t) throws Exception {
        long[] modePathTimestamp = t.getModelPathTimeStamps();
        byte[] value = t.getValue();
        ByteBuffer bb = ByteBuffer.allocate(1 + 1 + modePathTimestamp.length * 8 + value.length);
        bb.put((byte) 0); // version
        if (modePathTimestamp.length > Byte.MAX_VALUE) {
            throw new RuntimeException("Path length greater that " + Byte.MAX_VALUE + " are NOT supported by this marshaller.");
        }
        bb.put((byte) modePathTimestamp.length);
        for (int i = 0; i < modePathTimestamp.length; i++) {
            bb.putLong(modePathTimestamp[i]);
        }
        bb.put(value);
        return bb.array();
    }
}

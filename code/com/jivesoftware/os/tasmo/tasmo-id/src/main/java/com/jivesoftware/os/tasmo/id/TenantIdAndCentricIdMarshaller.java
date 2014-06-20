package com.jivesoftware.os.tasmo.id;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;

public class TenantIdAndCentricIdMarshaller implements TypeMarshaller<TenantIdAndCentricId> {

    @Override
    public TenantIdAndCentricId fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(TenantIdAndCentricId tenantIdAndCentricId) throws Exception {
        return toLexBytes(tenantIdAndCentricId);
    }

    @Override
    public TenantIdAndCentricId fromLexBytes(byte[] bytes) throws Exception {
        Preconditions.checkNotNull(bytes);
        int l = bytes[0];
        return new TenantIdAndCentricId(
                new TenantId(new String(copy(bytes, 1 + l, bytes.length - 1 - l), "UTF-8")),
                new Id(copy(bytes, 1, l)));
    }

    @Override
    public byte[] toLexBytes(TenantIdAndCentricId tenantIdAndCentricId) throws Exception {
        return join(tenantIdAndCentricId.getCentricId().toLengthPrefixedBytes(),
                tenantIdAndCentricId.getTenantId().toStringForm().getBytes("UTF-8"));
    }

    private byte[] copy(byte[] src, int _start, int _length) {
        byte[] copy = new byte[_length];
        System.arraycopy(src, _start, copy, 0, _length);
        return copy;
    }

    private byte[] join(byte[] a, byte[] b) {
        byte[] newSrc = new byte[a.length + b.length];
        System.arraycopy(a, 0, newSrc, 0, a.length);
        System.arraycopy(b, 0, newSrc, a.length, b.length);
        return newSrc;
    }
}

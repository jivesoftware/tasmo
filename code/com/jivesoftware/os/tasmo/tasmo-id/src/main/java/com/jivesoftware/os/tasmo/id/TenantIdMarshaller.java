package com.jivesoftware.os.tasmo.id;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;

/**
 *
 *
 */
public class TenantIdMarshaller implements TypeMarshaller<TenantId> {

    @Override
    public TenantId fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
}

    @Override
    public byte[] toBytes(TenantId tenantId) throws Exception {
        return toLexBytes(tenantId);
    }

    @Override
    public TenantId fromLexBytes(byte[] bytes) throws Exception {
        Preconditions.checkNotNull(bytes);
        return new TenantId(new String(bytes, "UTF-8"));
    }

    @Override
    public byte[] toLexBytes(TenantId tenantId) throws Exception {
        return tenantId.toStringForm().getBytes("UTF-8");
    }
}

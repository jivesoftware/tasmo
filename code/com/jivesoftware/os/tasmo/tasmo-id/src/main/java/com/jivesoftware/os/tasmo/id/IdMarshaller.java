/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;


/**
 *
 */
public class IdMarshaller implements TypeMarshaller<Id> {

    public IdMarshaller() {
    }

    @Override
    public Id fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(Id t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public Id fromLexBytes(byte[] lexBytes) throws Exception {
        return new Id(lexBytes);
    }

    @Override
    public byte[] toLexBytes(Id t) throws Exception {
        return t.toBytes();
    }
}

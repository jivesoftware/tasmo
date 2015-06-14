/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.TypeMarshaller;
import java.io.ByteArrayInputStream;

/**
 *
 */
public class JsonObjectNodeMarshaller implements TypeMarshaller<ObjectNode> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JsonObjectNodeMarshaller() {
    }

    @Override
    public ObjectNode fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(ObjectNode t) throws Exception {
        return toLexBytes(t);
    }

    @Override
    public ObjectNode fromLexBytes(byte[] lexBytes) throws Exception {
        if (lexBytes == null) {
            return null;
        }
        return MAPPER.readValue(new ByteArrayInputStream(lexBytes), ObjectNode.class);
    }

    @Override
    public byte[] toLexBytes(ObjectNode t) throws Exception {
        return MAPPER.writeValueAsBytes(t);
    }
}

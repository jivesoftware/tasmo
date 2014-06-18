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
import java.util.Random;

import com.google.common.io.BaseEncoding;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.FileAssert.fail;


public class IdTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final BaseEncoding CODER = BaseEncoding.base32().lowerCase().omitPadding();

    @Test
    public void idStringTest() throws Exception {
        final Random r = new Random(1234);
        for (int i = 0; i < 100; i++) {
            long id = Math.abs(r.nextLong());
            Id wid = new Id(id);
            Id rid = MAPPER.readValue(MAPPER.writeValueAsString(wid), Id.class);

            Assert.assertEquals(rid, wid, "Failed to map Id through String: " + id);
        }
    }

    @Test
    public void idBytesTest() throws Exception {
        final Random r = new Random(1234);
        for (int i = 0; i < 100; i++) {
            long id = Math.abs(r.nextLong());
            Id wid = new Id(id);
            Id rid = MAPPER.readValue(MAPPER.writeValueAsBytes(wid), Id.class);

            Assert.assertEquals(rid, wid, "Failed to map Id through bytes: " + id);
        }
    }

    @Test
    public void mapsToString() throws Exception {
        Id id = new Id(2882);

        Assert.assertEquals(MAPPER.writeValueAsString(id), "\"aaaaaaaaaafue\"");
    }

    @Test
    public void mapsFromString() throws Exception {
        String id = "\"aaaaaaaaaafue\"";

        Assert.assertEquals(MAPPER.readValue(id, Id.class), new Id(2882));
    }

    @Test
    public void mapsFromObject() throws Exception {
        String json = "{ \"id\": \"aaaaaaaaaafue\"}";

        Assert.assertEquals(MAPPER.readValue(json, Id.class), new Id(2882));
    }

    @Test
    public void testStringConstructorReturnsLong() throws Exception {
        Id stringId = new Id("eight");
        Assert.assertTrue(isLong(stringId.toStringForm()));

        Id fromStringForm = new Id(stringId.toStringForm());
        Assert.assertEquals(stringId, fromStringForm);
    }

    @Test
    public void mapsFromLegacyObject() throws Exception {
        Id legacy = new Id("eight", true);
        String json = "{ \"id\": \"" + legacy.toStringForm() + "\"}";
        Assert.assertEquals(MAPPER.readValue(json, Id.class), legacy);
    }

    @Test
    public void test() {
        Id numberId = new Id(1);
        Id stringFormId = new Id(numberId.toStringForm());

        Assert.assertEquals(numberId, stringFormId);
        Assert.assertEquals(numberId.toStringForm(), stringFormId.toStringForm());
    }

    private static boolean isLong(String stringForm) {
        byte [] id = CODER.decode(stringForm);
        return (id.length == 8);
    }
}

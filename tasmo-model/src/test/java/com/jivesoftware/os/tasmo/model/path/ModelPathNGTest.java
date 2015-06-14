/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.model.path;

import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.api.UtilLexMarshaller;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 * @author jonathan
 */
public class ModelPathNGTest {

    public ModelPathNGTest() {
    }

    /**

    /**
     * Test of orderIdsToLexSortableByteArrayOutputStream method, of class ModelPath.
     */
    @Test
    public void testOrderIdsToLexSortableByteArrayOutputStream() throws Exception {
        System.out.println("orderIdsToLexSortableByteArrayOutputStream");

        byte[] original = new byte[] { 1, 2, 3, 4 };
        byte[] inverted = UtilLexMarshaller.invert(original);
        byte[] back = UtilLexMarshaller.invert(inverted);
        assertEquals(original, back);

        long[] orderIds = new long[] {
            1L,
            2L,
            3L
        };

        //        String[] paths = new String[]{
        //            "A.pid.0.desc.f.ref.B|B.id.1.desc.f.ref.D|D.id.2.desc.f",
        //            "A.pid.0.asc.f.ref.B|B.id.1.asc.f.ref.D|D.id.2.asc.f",
        //            "A.pid.0.desc.f.ref.B|B.id.1.asc.f.ref.D|D.id.2.desc.f",
        //            "A.pid.1.asc.f.ref.B|B.id.2.asc.f.ref.D|D.id.0.asc.f",
        //        };
        //        for(String path:paths) {
        //
        //            ModelPath instance = ModelPath.buildPath(path);
        //            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        //            instance.orderIdsToLexSortableByteArrayOutputStream(orderIds, byteArrayOutputStream);
        //            OrderId[] orderIds2 = instance.lexSortableBytesToOrderIds(byteArrayOutputStream.toByteArray());
        //            assertEqualsNoOrder(orderIds, orderIds2);
        //        }

    }

}

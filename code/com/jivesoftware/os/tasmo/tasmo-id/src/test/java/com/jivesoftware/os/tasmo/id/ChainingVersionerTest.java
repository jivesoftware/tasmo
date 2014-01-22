/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.id;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class ChainingVersionerTest {


    /**
     * Test of nextVersion method, of class ChainingVersioner.
     */
    @Test
    public void testNextVersion() {
        System.out.println("nextVersion");
        ChainedVersion version = new ChainedVersion("1", "2");
        ChainingVersioner chainingVersioner = new ChainingVersioner();
        ChainedVersion result = chainingVersioner.nextVersion(version);
        Assert.assertEquals(result.getPriorVersion(), version.getVersion());
        version = result;
        result = chainingVersioner.nextVersion(version);
        Assert.assertEquals(result.getPriorVersion(), version.getVersion());
    }
}

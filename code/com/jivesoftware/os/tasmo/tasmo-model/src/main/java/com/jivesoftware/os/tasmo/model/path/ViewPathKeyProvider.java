/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.path;

import com.google.common.base.Joiner;

/**
 *
 * @author pete
 */
public class ViewPathKeyProvider {

    public final int pathKeyForClasses(String[] classes) {
        return Joiner.on('.').join(classes).hashCode();
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.model.path;

/**
 *
 * @author pete
 */
public class ViewPathKeyProvider {

    static char[] dot = new char[]{'.'};

    public final int pathKeyForClasses(String[] classes) {
        int hash = 0;
        for (int i = 0; i < classes.length; i++) {
            if (i != 0) {
                hash = hashCode(hash, dot);
            }
            hash = hashCode(hash, classes[i].toCharArray());
        }
        return hash;
        //return Joiner.on('.').join(classes).hashCode();
    }

    public int hashCode(int h, char[] value) {
        if (value.length > 0) {
            char val[] = value;

            for (int i = 0; i < value.length; i++) {
                h = 31 * h + val[i];
            }
        }
        return h;
    }
}

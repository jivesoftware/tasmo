/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.configuration.views;

import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ViewPathDictionary;

/**
 * Tuple used to pass the path and associated dictionary around together
 *
 */
public class PathAndDictionary {

    private final ModelPath path;
    private final ViewPathDictionary dictionary;

    public PathAndDictionary(ModelPath path, ViewPathDictionary dictionary) {
        this.path = path;
        this.dictionary = dictionary;
    }

    public ViewPathDictionary getDictionary() {
        return dictionary;
    }

    public ModelPath getPath() {
        return path;
    }
}

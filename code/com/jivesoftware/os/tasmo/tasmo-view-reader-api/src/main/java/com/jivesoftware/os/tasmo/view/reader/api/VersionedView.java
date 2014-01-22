/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.view.reader.api;

public interface VersionedView<V> {

    V getView();

    ViewVersion getVersion();
}

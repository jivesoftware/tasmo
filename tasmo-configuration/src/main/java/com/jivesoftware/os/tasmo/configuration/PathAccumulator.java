/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Accumulates and publishes model path state while example view data is being traversed.
 */
public class PathAccumulator implements PathCallback {

    private final LinkedList<TypedField> path = new LinkedList<>();
    final AccumlatedPath accumlatedPath;

    public static interface AccumlatedPath {
        void path(List<TypedField> path);
    }

    /**
     *
     * @param accumlatedPath
     */
    public PathAccumulator(AccumlatedPath accumlatedPath) {
        this.accumlatedPath = accumlatedPath;
    }

    @Override
    public void push(Set<String> fieldType, ValueType valueType, String... fieldNames) {
        path.addLast(new TypedField(fieldType, fieldNames, valueType));
    }

    @Override
    public void pop() {
        accumlatedPath.path(new ArrayList<>(path));
        path.removeLast();
    }

}

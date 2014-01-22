/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */

package com.jivesoftware.os.tasmo.configuration;

import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public interface PathCallback {

    void push(Set<String> fieldType, ValueType valueType, String... fieldNames);

    void pop();

}

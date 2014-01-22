/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.jivesoftware.os.tasmo.id.Id;

/**
 *
 */
public class User {

    public String instanceId = "User_" + new Id(1).toStringForm();
    public String firstName = "firstName";
    public String lastName = "lastName";
}

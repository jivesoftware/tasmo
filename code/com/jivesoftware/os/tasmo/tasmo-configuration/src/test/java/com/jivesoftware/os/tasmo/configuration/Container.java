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
public class Container {

    public String instanceId = "Container_" + new Id(1).toStringForm();
    public String parent = "Container_" + new Id(1).toStringForm();
    public String name = "name";
    public String[] tags = new String[] { "Tag_" + new Id(1).toStringForm() };

    /*
     * { eventId: asdfasdf actor: User_2345234 tenant: asdfasdfsdfa Container: { instancei } }
     */
}

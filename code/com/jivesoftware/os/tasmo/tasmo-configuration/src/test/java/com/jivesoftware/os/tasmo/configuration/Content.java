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
public class Content {

    public String instanceId = "Content_" + new Id(1).toStringForm();
    public String parent = "Container_" + new Id(1).toStringForm();
    public String title = "title";
    public String body = "body";
    public String[] authors = new String[] { "User_" + new Id(1).toStringForm() };
    public String[] tags = new String[] { "Tag_" + new Id(1).toStringForm() };
}

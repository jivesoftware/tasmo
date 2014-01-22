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
public class ContentView {
    public String objectId = "Content_" + new Id(1).toStringForm();
    public String title = "title";
    public Container parent = new Container();

    public static class Container {
        public String objectId = "Container_" + new Id(1).toStringForm();
        public Tag[] tags = new Tag[] { new Tag() };
    }

    public static class Tag {
        public String objectId = "Tag_" + new Id(1).toStringForm();
        public String name = "name";
    }

}

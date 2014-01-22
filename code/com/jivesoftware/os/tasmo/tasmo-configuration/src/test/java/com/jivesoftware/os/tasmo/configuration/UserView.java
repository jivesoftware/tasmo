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
public class UserView {

    public String objectId = "User_" + new Id(1).toStringForm();
    public String firstName = "firstName";
    public Content[] all_authors = new Content[] { new Content() };

    public static class Content {
        public String objectId = "Content_" + new Id(1).toStringForm();
        public String title = "title";
        public Container parent = new Container();
    }

    public static class Container {
        public String objectId = "Container_" + new Id(1).toStringForm();
        public ParentContainer parent = new ParentContainer();
    }

    public static class ParentContainer {
        public String objectId = "Container_" + new Id(2).toStringForm();
        public String name = "containerName2";
        public Tag[] tags = new Tag[] { new Tag() };
    }

    public static class Tag {
        public String objectId = "Tag_" + new Id(1).toStringForm();
        public String name = "tagName";
        public TagAuthor author = new TagAuthor();
    }

    public static class TagAuthor {
        public String objectId = "User_" + new Id(2).toStringForm();
        public String lastName = "tagAuthorLastName";
    }

}

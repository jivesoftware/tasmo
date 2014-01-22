package com.jivesoftware.os.tasmo.configuration;

import com.jivesoftware.os.tasmo.id.Id;

public class ContainerView {

    public String objectId = "Container_" + new Id(1).toStringForm();
    public String name = "name";
    public Content[] all_parent = new Content[] { new Content() };

    public static class Content {
        public String objectId = "Content_" + new Id(1).toStringForm();
        public User[] authors = new User[] { new User() };
    }

    public static class User {
        public String objectId = "User_" + new Id(1).toStringForm();
        public String firstName = "firstName";
        public String lastName = "lastName";
    }
}

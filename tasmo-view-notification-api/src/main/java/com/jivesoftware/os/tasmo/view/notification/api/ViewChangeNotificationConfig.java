package com.jivesoftware.os.tasmo.view.notification.api;

import org.merlin.config.Config;
import org.merlin.config.annotations.Property;
import org.merlin.config.defaults.Default;


public interface ViewChangeNotificationConfig extends Config {

    @Default("view-change")
    @Property("topicPrefix")
    String getTopicPrefix();

    void setTopicPrefix(String topicPrefix);

}
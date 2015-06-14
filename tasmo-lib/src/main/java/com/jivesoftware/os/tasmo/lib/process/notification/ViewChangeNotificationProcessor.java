package com.jivesoftware.os.tasmo.lib.process.notification;

import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public interface ViewChangeNotificationProcessor {

    void process(WrittenEventContext batchContext, WrittenEvent writtenEvent) throws Exception;

}

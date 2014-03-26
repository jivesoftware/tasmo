package com.jivesoftware.os.tasmo.view.notification.lib;

import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public interface ViewChangeNotificationProcessor {

    void process(ModifiedViewProvider modifiedViewProvider, WrittenEvent writtenEvent) throws Exception;

}

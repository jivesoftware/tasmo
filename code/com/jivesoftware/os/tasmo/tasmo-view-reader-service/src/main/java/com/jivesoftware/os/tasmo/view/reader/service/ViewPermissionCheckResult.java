package com.jivesoftware.os.tasmo.view.reader.service;

import com.jivesoftware.os.tasmo.id.Id;
import java.util.Set;

public interface ViewPermissionCheckResult {

    Set<Id> allowed();

    Set<Id> denied();

    Set<Id> unknown();
}

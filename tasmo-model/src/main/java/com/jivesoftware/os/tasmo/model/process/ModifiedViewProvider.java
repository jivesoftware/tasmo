package com.jivesoftware.os.tasmo.model.process;

import java.util.Set;

public interface ModifiedViewProvider {

    Set<ModifiedViewInfo> getModifiedViews();

    void add(ModifiedViewInfo viewId);
}

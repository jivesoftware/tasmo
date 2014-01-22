package com.jivesoftware.os.tasmo.model.process;

import java.util.HashSet;
import java.util.Set;

public class InMemoryModifiedViewProvider implements ModifiedViewProvider {

    Set<ModifiedViewInfo> modifiedViews = new HashSet<>();

    @Override
    public Set<ModifiedViewInfo> getModifiedViews() {
        return modifiedViews;
    }

    @Override
    public void add(ModifiedViewInfo view) {
        modifiedViews.add(view);
    }
}

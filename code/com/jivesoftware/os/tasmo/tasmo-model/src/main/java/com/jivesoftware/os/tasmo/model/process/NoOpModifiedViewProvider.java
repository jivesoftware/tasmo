package com.jivesoftware.os.tasmo.model.process;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class NoOpModifiedViewProvider implements ModifiedViewProvider {

    @Override
    public Set<ModifiedViewInfo> getModifiedViews() {
        return new HashSet<>();
    }

    @Override
    public void add(ModifiedViewInfo viewId) {
    }
}

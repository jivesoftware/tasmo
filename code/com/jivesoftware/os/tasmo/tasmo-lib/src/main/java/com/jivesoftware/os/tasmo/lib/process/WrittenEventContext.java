package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import java.util.Set;

public class WrittenEventContext {

    private final ModifiedViewProvider modifiedViewProvider;
    private final Set<ObjectId> transitioning;

    public WrittenEventContext(ModifiedViewProvider modifiedViewProvider, Set<ObjectId> transitioning) {
        this.modifiedViewProvider = modifiedViewProvider;
        this.transitioning = transitioning;
    }

    public ModifiedViewProvider getModifiedViewProvider() {
        return modifiedViewProvider;
    }

    public Set<ObjectId> getTransitioning() {
        return transitioning;
    }

}

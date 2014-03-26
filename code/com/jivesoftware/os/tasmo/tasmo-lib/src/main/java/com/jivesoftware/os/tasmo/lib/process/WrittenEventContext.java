package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;

public class WrittenEventContext {

    private final ModifiedViewProvider modifiedViewProvider;

    public WrittenEventContext(ModifiedViewProvider modifiedViewProvider) {
        this.modifiedViewProvider = modifiedViewProvider;
    }

    public ModifiedViewProvider getModifiedViewProvider() {
        return modifiedViewProvider;
    }

}

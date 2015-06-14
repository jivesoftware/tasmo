package com.jivesoftware.os.tasmo.model.path;

public interface ViewPathKeyProvider {

    long pathKeyHashcode(String[] classes);

    long modelPathHashcode(String modelPathId);
}

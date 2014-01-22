package com.jivesoftware.os.tasmo.view.reader.api;

import java.util.Map;

/**
 * An interface that marks something that can be represented as a nested key-value Map
 * (everything in the world ;) )
 */
public interface JSONizable {
    Map<String, Object> asMap();
}

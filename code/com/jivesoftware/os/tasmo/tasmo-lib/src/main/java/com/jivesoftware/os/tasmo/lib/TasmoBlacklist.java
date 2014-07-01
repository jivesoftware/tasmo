package com.jivesoftware.os.tasmo.lib;

import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class TasmoBlacklist {

    private final Set<Long> backlistedEventIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    public void blacklistEventId(long eventId) {
        backlistedEventIds.add(eventId);
    }

    public void whitelistEventId(long eventId) {
        backlistedEventIds.remove(eventId);
    }

    public void clear() {
        backlistedEventIds.clear();
    }

    public boolean blacklisted(WrittenEvent event) {
        if (backlistedEventIds.contains(event.getEventId())) {
            return true;
        }
        return false;
    }
}

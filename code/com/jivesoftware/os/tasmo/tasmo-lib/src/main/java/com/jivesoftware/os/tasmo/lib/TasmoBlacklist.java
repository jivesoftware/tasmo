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

    private final Set<Long> blacklistedEventIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    public void blacklistEventId(long eventId) {
        blacklistedEventIds.add(eventId);
    }

    public void whitelistEventId(long eventId) {
        blacklistedEventIds.remove(eventId);
    }

    public void clear() {
        blacklistedEventIds.clear();
    }

    public boolean blacklisted(WrittenEvent event) {
        if (blacklistedEventIds.contains(event.getEventId())) {
            return true;
        }
        return false;
    }
}

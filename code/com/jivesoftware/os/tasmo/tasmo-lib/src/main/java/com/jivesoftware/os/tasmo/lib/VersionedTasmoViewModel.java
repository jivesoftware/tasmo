package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final Map<String, InitiateTraversal> dispatchers;
    private final SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel;
    private final Set<String> notifiableViews;

    public VersionedTasmoViewModel(ChainedVersion version,
        Map<String, InitiateTraversal> dispatchers,
        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel,
        Set<String> notifiableViews) {
        this.version = version;
        this.dispatchers = dispatchers;
        this.eventModel = eventModel;
        this.notifiableViews = notifiableViews;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public Map<String, InitiateTraversal> getDispatchers() {
        return dispatchers;
    }

    public SetMultimap<String, TasmoViewModel.FieldNameAndType> getEventModel() {
        return eventModel;
    }

    public Set<String> getNotifiableViews() {
        return notifiableViews;
    }

    @Override
    public String toString() {
        return "VersionedViewTasmoModel{"
            + "version=" + version
            + ", dispatchers=" + dispatchers
            + '}';
    }
}

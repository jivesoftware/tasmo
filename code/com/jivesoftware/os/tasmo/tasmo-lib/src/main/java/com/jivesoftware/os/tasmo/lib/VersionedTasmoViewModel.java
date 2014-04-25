package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final ListMultimap<String, InitiateTraversal> dispatchers;
    private final ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel;
    private final Set<String> notifiableViews;

    public VersionedTasmoViewModel(ChainedVersion version,
            ListMultimap<String, InitiateTraversal> dispatchers,
            ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel,
            Set<String> notifiableViews) {
        this.version = version;
        this.dispatchers = dispatchers;
        this.eventModel = eventModel;
        this.notifiableViews = notifiableViews;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public ListMultimap<String, InitiateTraversal> getDispatchers() {
        return dispatchers;
    }

    public ListMultimap<String, TasmoViewModel.FieldNameAndType> getEventModel() {
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

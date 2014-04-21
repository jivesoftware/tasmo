package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;

/**
 *
 * @author jonathan.colt
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final ListMultimap<String, InitiateTraversal> dispatchers;
    private final ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel;

    public VersionedTasmoViewModel(ChainedVersion version,
            ListMultimap<String, InitiateTraversal> dispatchers,
            ListMultimap<String, TasmoViewModel.FieldNameAndType> eventModel) {
        this.version = version;
        this.dispatchers = dispatchers;
        this.eventModel = eventModel;
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

    @Override
    public String toString() {
        return "VersionedViewTasmoModel{"
                + "version=" + version
                + ", dispatchers=" + dispatchers
                + '}';
    }
}

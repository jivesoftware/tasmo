package com.jivesoftware.os.tasmo.lib.model;

import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateReadTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class VersionedTasmoViewModel {

    private final ChainedVersion version;
    private final Map<String, InitiateWriteTraversal> writeTraversers;
    private final Map<String, InitiateWriteTraversal> centricWriteTraversers;
    private final Map<String, InitiateReadTraversal> readTraversers;
    private final SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel;
    private final Set<String> notifiableViews;

    public VersionedTasmoViewModel(ChainedVersion version,
        Map<String, InitiateWriteTraversal> writeTraversers,
        Map<String, InitiateWriteTraversal> centricWriteTraversers,
        Map<String, InitiateReadTraversal> readTraversers,
        SetMultimap<String, TasmoViewModel.FieldNameAndType> eventModel,
        Set<String> notifiableViews) {
        this.version = version;
        this.writeTraversers = writeTraversers;
        this.centricWriteTraversers = centricWriteTraversers;
        this.readTraversers = readTraversers;
        this.eventModel = eventModel;
        this.notifiableViews = notifiableViews;
    }

    public ChainedVersion getVersion() {
        return version;
    }

    public Map<String, InitiateWriteTraversal> getWriteTraversers() {
        return writeTraversers;
    }
    public Map<String, InitiateWriteTraversal> getCentricWriteTraversers() {
        return centricWriteTraversers;
    }

    public Map<String, InitiateReadTraversal> getReadTraversers() {
        return readTraversers;
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
            + ", dispatchers=" + writeTraversers
            + '}';
    }
}

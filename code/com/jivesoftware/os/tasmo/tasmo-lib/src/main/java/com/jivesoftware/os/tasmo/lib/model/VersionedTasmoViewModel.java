package com.jivesoftware.os.tasmo.lib.model;

import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateReadTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import java.util.Collections;
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
        if (writeTraversers == null) {
            return null;
        } else {
            return Collections.unmodifiableMap(writeTraversers);
        }
    }
    public Map<String, InitiateWriteTraversal> getCentricWriteTraversers() {
        if (centricWriteTraversers == null) {
            return null;
        } else {
            return Collections.unmodifiableMap(centricWriteTraversers);
        }
    }

    public Map<String, InitiateReadTraversal> getReadTraversers() {
        if (readTraversers == null) {
            return null;
        } else {
            return Collections.unmodifiableMap(readTraversers);
        }
    }

    public SetMultimap<String, TasmoViewModel.FieldNameAndType> getEventModel() {
        return eventModel;
    }

    public Set<String> getNotifiableViews() {
        if (notifiableViews == null) {
            return null;
        } else {
            return Collections.unmodifiableSet(notifiableViews);
        }
    }

    @Override
    public String toString() {
        return "VersionedViewTasmoModel{"
            + "version=" + version
            + ", dispatchers=" + writeTraversers
            + '}';
    }
}

package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;

public class WrittenEventContext {

    private final long eventId;
    private final Id actorId;
    private final WrittenEvent event;
    private final WrittenEventProvider writtenEventProvider;
    private final FieldValueReader fieldValueReader;
    private final ReferenceTraverser referenceTraverser;
    private final ModifiedViewProvider modifiedViewProvider;
    private final CommitChange commitChange;
    private final TasmoEdgeReport tasmoEdgeReport;
    private final TasmoProcessingStats processingStats;

    public int valuePaths; // hack
    public int refPaths; // hack
    public int backRefPaths; // hack
    public int fanDepth; // hack
    public int fanBreath; // hack
    public int readLeaves; // hack
    public int changes;

    public WrittenEventContext(long eventId,
            Id actorId,
            WrittenEvent event,
            WrittenEventProvider writtenEventProvider,
            FieldValueReader fieldValueReader,
            ReferenceTraverser referenceTraverser,
            ModifiedViewProvider modifiedViewProvider,
            CommitChange commitChange,
            TasmoEdgeReport tasmoEdgeReport,
            TasmoProcessingStats processingStats) {
        this.eventId = eventId;
        this.actorId = actorId;
        this.event = event;
        this.writtenEventProvider = writtenEventProvider;
        this.fieldValueReader = fieldValueReader;
        this.referenceTraverser = referenceTraverser;
        this.modifiedViewProvider = modifiedViewProvider;
        this.commitChange = commitChange;
        this.tasmoEdgeReport = tasmoEdgeReport;
        this.processingStats = processingStats;
    }

    public long getEventId() {
        return eventId;
    }

    public Id getActorId() {
        return actorId;
    }

    public TasmoProcessingStats getProcessingStats() {
        return processingStats;
    }

    public WrittenEvent getEvent() {
        return event;
    }

    public WrittenEventProvider getWrittenEventProvider() {
        return writtenEventProvider;
    }

    public FieldValueReader getFieldValueReader() {
        return fieldValueReader;
    }

    public ReferenceTraverser getReferenceTraverser() {
        return referenceTraverser;
    }

    public ModifiedViewProvider getModifiedViewProvider() {
        return modifiedViewProvider;
    }

    public CommitChange getCommitChange() {
        return commitChange;
    }

    public TasmoEdgeReport getTasmoEdgeReport() {
        return tasmoEdgeReport;
    }

}

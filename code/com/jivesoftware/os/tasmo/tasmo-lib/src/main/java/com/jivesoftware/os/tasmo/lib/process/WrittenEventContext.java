package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.read.FieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;

public class WrittenEventContext {

    private final long eventId;
    private final Id actorId;
    private final WrittenEvent event;
    private final WrittenEventProvider writtenEventProvider;
    private final ConcurrencyChecker concurrencyChecker;
    private final ReferenceStore referenceStore;
    private final FieldValueReader fieldValueReader;
    private final ReferenceTraverser referenceTraverser;
    private final ModifiedViewProvider modifiedViewProvider;
    private final CommitChange commitChange;
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
        ConcurrencyChecker concurrencyChecker,
        ReferenceStore referenceStore,
        FieldValueReader fieldValueReader,
        ReferenceTraverser referenceTraverser,
        ModifiedViewProvider modifiedViewProvider,
        CommitChange commitChange,
        TasmoProcessingStats processingStats) {
        this.eventId = eventId;
        this.actorId = actorId;
        this.event = event;
        this.writtenEventProvider = writtenEventProvider;
        this.concurrencyChecker = concurrencyChecker;
        this.referenceStore = referenceStore;
        this.fieldValueReader = fieldValueReader;
        this.referenceTraverser = referenceTraverser;
        this.modifiedViewProvider = modifiedViewProvider;
        this.commitChange = commitChange;
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

    public ConcurrencyChecker getConcurrencyChecker() {
        return concurrencyChecker;
    }

    public ReferenceStore getReferenceStore() {
        return referenceStore;
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

}

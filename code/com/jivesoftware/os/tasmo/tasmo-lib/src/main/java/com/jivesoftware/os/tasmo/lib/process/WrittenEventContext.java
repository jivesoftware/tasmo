package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;

public class WrittenEventContext {

    private final WrittenEvent event;
    private final ModifiedViewProvider modifiedViewProvider;
    private final FieldValueReader fieldValueReader;
    private final CommitChange commitChange;
    private final TasmoEdgeReport tasmoEdgeReport;

    public int paths; // hack
    public int fanDepth; // hack
    public int fanBreath; // hack
    public int readLeaves; // hack
    public int changes;

    public WrittenEventContext(WrittenEvent event,
            ModifiedViewProvider modifiedViewProvider,
            FieldValueReader fieldValueReader,
            CommitChange commitChange,
            TasmoEdgeReport tasmoEdgeReport) {
        this.event = event;
        this.modifiedViewProvider = modifiedViewProvider;
        this.fieldValueReader = fieldValueReader;
        this.commitChange = commitChange;
        this.tasmoEdgeReport = tasmoEdgeReport;
    }

    public WrittenEvent getEvent() {
        return event;
    }

    public ModifiedViewProvider getModifiedViewProvider() {
        return modifiedViewProvider;
    }

    public FieldValueReader getFieldValueReader() {
        return fieldValueReader;
    }

    public CommitChange getCommitChange() {
        return commitChange;
    }

    public TasmoEdgeReport getTasmoEdgeReport() {
        return tasmoEdgeReport;
    }

}

package com.jivesoftware.os.tasmo.lib.process;

import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.report.TasmoEdgeReport;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.WrittenEvent;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;

public class WrittenEventContext {

    private final WrittenEvent event;
    private final WrittenEventProvider writtenEventProvider;
    private final FieldValueReader fieldValueReader;
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

    public WrittenEventContext(WrittenEvent event,
            WrittenEventProvider writtenEventProvider,
            FieldValueReader fieldValueReader,
            ModifiedViewProvider modifiedViewProvider,
            CommitChange commitChange,
            TasmoEdgeReport tasmoEdgeReport,
            TasmoProcessingStats processingStats) {
        this.event = event;
        this.writtenEventProvider = writtenEventProvider;
        this.fieldValueReader = fieldValueReader;
        this.modifiedViewProvider = modifiedViewProvider;
        this.commitChange = commitChange;
        this.tasmoEdgeReport = tasmoEdgeReport;
        this.processingStats = processingStats;
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

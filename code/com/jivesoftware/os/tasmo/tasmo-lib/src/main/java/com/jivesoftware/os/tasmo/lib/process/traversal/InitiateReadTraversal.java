package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewInfo;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InitiateReadTraversal {

    private final Set<String> rootingEventClassNames;
    private final List<PathAtATimeStepStreamerFactory> pathTraversers;
    private final int pathLength;
    private final boolean idCentric;

    public InitiateReadTraversal(Set<String> rootingEventClassNames,
        List<PathAtATimeStepStreamerFactory> pathTraversers,
        int pathLength,
        boolean idCentric) {
        this.rootingEventClassNames = rootingEventClassNames;
        this.pathTraversers = pathTraversers;
        this.pathLength = pathLength;
        this.idCentric = idCentric;
    }

    public void read(ReferenceTraverser referenceTraverser,
        FieldValueReader fieldValueReader,
        TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId id,
        CommitChange commitChange) throws Exception {

        ModifiedViewProvider modifiedViewProvider = new ModifiedViewProvider() {

            @Override
            public Set<ModifiedViewInfo> getModifiedViews() {
                return new HashSet<>();
            }

            @Override
            public void add(ModifiedViewInfo viewId) {
            }
        };

        TasmoProcessingStats processingStats = new TasmoProcessingStats();

        WrittenEventContext writtenEventContext = new WrittenEventContext(0,
            Id.NULL,
            null,
            new JsonWrittenEventProvider(),
            null,
            null,
            fieldValueReader,
            referenceTraverser,
            modifiedViewProvider,
            commitChange,
            processingStats);

        PathTraversalContext context = new PathTraversalContext(1, false);
        PathContext pathContext = new PathContext(pathLength);
        LeafContext leafContext = new ReadLeafContext();
        for (PathAtATimeStepStreamerFactory pathTraverser : pathTraversers) {
            StepStreamer stepStreamer = pathTraverser.create();

            for (String rootEventClassName : rootingEventClassNames) {
                PathId pathId = new PathId(new ObjectId(rootEventClassName, id.getId()), 1);
                stepStreamer.stream(tenantIdAndCentricId, writtenEventContext, context, pathContext, leafContext, pathId);
            }
        }
        List<ViewFieldChange> took = context.takeChanges();
        List<ViewFieldChange> changes = new ArrayList<>();
        for (ViewFieldChange t : took) {
            changes.add(new ViewFieldChange(t.getEventId(),
                t.getActorId(),
                t.getType(),
                id,
                t.getModelPath(),
                t.getModelPathIdHashcode(),
                t.getModelPathInstanceIds(),
                t.getModelPathVersions(),
                t.getModelPathTimestamps(),
                t.getValue(),
                t.getTimestamp()));
        }
        commitChange.commitChange(writtenEventContext, tenantIdAndCentricId, changes);
    }
}

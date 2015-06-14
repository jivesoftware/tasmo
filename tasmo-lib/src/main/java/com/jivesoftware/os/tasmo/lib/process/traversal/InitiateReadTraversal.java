package com.jivesoftware.os.tasmo.lib.process.traversal;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel.ReadTraversalKey;
import com.jivesoftware.os.tasmo.lib.process.TasmoProcessingStats;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.read.FieldValueReader;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.process.JsonWrittenEventProvider;
import com.jivesoftware.os.tasmo.model.process.ModifiedViewProvider;
import com.jivesoftware.os.tasmo.model.process.NoOpModifiedViewProvider;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class InitiateReadTraversal {

    private final Set<String> rootingEventClassNames;
    private final Map<ReadTraversalKey, StepStreamerFactory> pathTraversers;

    public InitiateReadTraversal(Set<String> rootingEventClassNames,
        Map<ReadTraversalKey, StepStreamerFactory> pathTraversers) {
        this.rootingEventClassNames = rootingEventClassNames;
        this.pathTraversers = pathTraversers;
    }

    public void read(ReferenceTraverser referenceTraverser,
        FieldValueReader fieldValueReader,
        TenantId tenantId,
        Id actorId,
        Id userId,
        ObjectId id,
        CommitChange commitChange) throws Exception {

        ModifiedViewProvider modifiedViewProvider = new NoOpModifiedViewProvider();

        TasmoProcessingStats processingStats = new TasmoProcessingStats();

        WrittenEventContext writtenEventContext = new WrittenEventContext(0,
            actorId,
            userId,
            null,
            new JsonWrittenEventProvider(),
            null,
            null,
            fieldValueReader,
            referenceTraverser,
            modifiedViewProvider,
            commitChange,
            processingStats);

        TenantIdAndCentricId globalCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        TenantIdAndCentricId userCentricId = new TenantIdAndCentricId(tenantId, userId);

        PathTraversalContext context = new PathTraversalContext(1, false);
        for (Entry<ReadTraversalKey, StepStreamerFactory> e : pathTraversers.entrySet()) {
            ReadTraversalKey readTraversalKey = e.getKey();
            StepStreamerFactory stepStreamerFactory = e.getValue();
            StepStream stepStream = stepStreamerFactory.create();
            for (String rootEventClassName : rootingEventClassNames) {
                PathId pathId = new PathId(new ObjectId(rootEventClassName, id.getId()), 1);
                PathContext pathContext = new PathContext(readTraversalKey.getModelPath().getPathMemberSize());
                LeafContext leafContext = new ReadLeafContext();
                stepStream.stream(globalCentricId, userCentricId, writtenEventContext, context, pathContext, leafContext, pathId);
            }
        }
        List<ViewField> took = context.takeChanges();
        List<ViewField> allChanges = new ArrayList<>();
        for (ViewField t : took) {
            ViewField viewField = new ViewField(t.getEventId(),
                    t.getActorId(),
                    t.getUserId(),
                    t.getType(),
                    id,
                    t.getModelPath(),
                    t.getModelPathIdHashcode(),
                    t.getModelPathInstanceIds(),
                    t.getModelPathVersions(),
                    t.getModelPathTimestamps(),
                    t.getValue(),
                    t.getTimestamp());
            allChanges.add(viewField);
        }
        commitChange.commitChange(writtenEventContext, globalCentricId, allChanges);

    }
}
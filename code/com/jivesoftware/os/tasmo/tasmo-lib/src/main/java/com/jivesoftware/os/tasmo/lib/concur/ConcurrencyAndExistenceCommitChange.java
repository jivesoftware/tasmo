package com.jivesoftware.os.tasmo.lib.concur;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange.ViewFieldChangeType;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class ConcurrencyAndExistenceCommitChange implements CommitChange {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrencyStore concurrencyStore;
    private final CommitChange commitChange;

    public ConcurrencyAndExistenceCommitChange(ConcurrencyStore concurrencyStore,
            CommitChange commitChange) {
        this.concurrencyStore = concurrencyStore;
        this.commitChange = commitChange;
    }

    @Override
    public void commitChange(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId,
            List<ViewFieldChange> changes) throws CommitChangeException {

        Set<ObjectId> check = new HashSet<>();
        for (ViewFieldChange change : changes) {
            for (PathId ref : change.getModelPathInstanceIds()) {
                check.add(ref.getObjectId());
            }
        }

        Set<ObjectId> existence = concurrencyStore.getExistence(tenantIdAndCentricId, check);
        List<ViewFieldChange> acceptableChanges = new ArrayList<>();
        for (ViewFieldChange c : changes) {
            Set<ObjectId> ids = new HashSet<>();
            for (PathId ref : c.getModelPathInstanceIds()) {
                ids.add(ref.getObjectId());
            }
            if (c.getType() == ViewFieldChange.ViewFieldChangeType.remove) {
                acceptableChanges.add(c);
            } else if (existence.containsAll(ids)) {
                acceptableChanges.add(c);
            } else {
                traceLogging(ids, existence, c);
                acceptableChanges.add(new ViewFieldChange(c.getEventId(),
                        c.getActorId(),
                        ViewFieldChangeType.remove,
                        c.getViewObjectId(),
                        c.getModelPathId(),
                        c.getModelPathInstanceIds(),
                        c.getModelPathVersions(),
                        c.getModelPathTimestamps(),
                        c.getValue(),
                        c.getTimestamp()));
            }
        }

// TODO eval if this block is really necessary.
//
//        Set<FieldVersion> fieldVersions = new HashSet<>();
//        for (ViewFieldChange fieldChange : acceptableChanges) {
//            if (fieldChange.getType() == ViewFieldChange.ViewFieldChangeType.add) {
//                List<ReferenceWithTimestamp> versions = fieldChange.getModelPathVersions();
//                for (ReferenceWithTimestamp version : versions) {
//                    if (version != null) {
//                        // TODO this could grow rather large should consider adding batching!
//                        fieldVersions.add(new FieldVersion(version.getObjectId(), version.getFieldName(), version.getTimestamp()));
//                    }
//                }
//            }
//        }
//
//        if (!fieldVersions.isEmpty()) {
//            Set<FieldVersion> expected = new HashSet<>(fieldVersions);
//            Set<FieldVersion> was = concurrencyStore.checkIfModified(tenantIdAndCentricId, expected);
//            for (ViewFieldChange fieldChange : acceptableChanges) {
//                if (fieldChange.getType() == ViewFieldChange.ViewFieldChangeType.add) {
//
//                }
//            }
//
//            if (expected != was) {
//                PathConsistencyException pmofume = new PathConsistencyException(expected, was);
//                if (LOG.isTraceEnabled()) {
//                    LOG.trace("!!!!!!!!!!!!!!!!!!!!!!!!!!!! RETRY. ADD is based on inconsistent view. !!!!!!!!!!!!!!!!!!!!!!!!");
//
//                    StringBuilder sb = new StringBuilder();
//                    sb.append("BOOYA ~");
//                    sb.append(pmofume);
//                    sb.append("~");
//                    for (ViewFieldChange fieldChange : acceptableChanges) {
//                        sb.append(fieldChange);
//                        sb.append("~");
//                    }
//                    LOG.info(sb.toString());
//                }
//                throw pmofume;
//            }
//        }

        commitChange.commitChange(batchContext, tenantIdAndCentricId, acceptableChanges);

    }

    private void traceLogging(Set<ObjectId> ids, Set<ObjectId> existence, ViewFieldChange fieldChange) {
        if (LOG.isTraceEnabled()) {
            StringBuilder msg = new StringBuilder().append(" existence:");

            String sep = "";
            for (ObjectId id : ids) {
                msg.append(sep);
                msg.append(id).append(existence.contains(id) ? " exists" : " does not exist");
                sep = ",";
            }
            msg.append(" change:").append(fieldChange);

            if (LOG.isTraceEnabled()) {
                LOG.trace("WRITE BLOCKED DUE TO LACK OF EXISTANCE:" + msg.toString());
            }
        }
    }
}

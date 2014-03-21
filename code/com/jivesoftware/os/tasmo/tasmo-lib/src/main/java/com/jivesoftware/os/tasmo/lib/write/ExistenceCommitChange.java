package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import java.util.List;

public class ExistenceCommitChange implements CommitChange {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExistenceStore existenceStore;
    private final CommitChange commitChange;

    public ExistenceCommitChange(ExistenceStore existenceStore, CommitChange commitChange) {
        this.existenceStore = existenceStore;
        this.commitChange = commitChange;
    }

    @Override
    public void commitChange(TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {

        commitChange.commitChange(tenantIdAndCentricId, changes);

//        Set<ObjectId> check = new HashSet<>();
//        for (ViewFieldChange change : changes) {
//            for (ReferenceWithTimestamp ref : change.getModelPathInstanceIds()) {
//                check.add(ref.getObjectId());
//            }
//        }
//
//        Set<ObjectId> existence = existenceStore.getExistence(tenantIdAndCentricId.getTenantId(), check);
//
//        List<ViewFieldChange> filtered = new ArrayList<>();
//        for (ViewFieldChange fieldChange : changes) {
//            Set<ObjectId> ids = new HashSet<>();
//            for (ReferenceWithTimestamp ref : fieldChange.getModelPathInstanceIds()) {
//                ids.add(ref.getObjectId());
//            }
//
//            if (fieldChange.getType() == ViewFieldChange.ViewFieldChangeType.remove
//                    || existence.containsAll(ids)) {
//                filtered.add(fieldChange);
//            } else {
//                if (LOG.isDebugEnabled()) {
//                    StringBuilder msg = new StringBuilder("Ignoring ViewFieldChange: ").append(fieldChange).
//                            append(" due to nonexistent objects. ");
//
//                    String sep = "";
//                    for (ObjectId id : ids) {
//                        msg.append(sep);
//                        msg.append(id).append(existence.contains(id) ? " exists" : " does not exist");
//                        sep = ",";
//                    }
//
//                    LOG.debug(msg.toString());
//
//                }
//            }
//        }
//
//        System.out.println(Thread.currentThread() + " |--> ExistenceCommitChange:" + filtered);
//
//        commitChange.commitChange(tenantIdAndCentricId, filtered);
    }

}

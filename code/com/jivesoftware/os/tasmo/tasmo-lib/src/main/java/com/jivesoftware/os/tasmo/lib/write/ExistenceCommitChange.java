package com.jivesoftware.os.tasmo.lib.write;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ObjectId;
import com.jivesoftware.os.tasmo.id.TenantIdAndCentricId;
import com.jivesoftware.os.tasmo.lib.exists.ExistenceStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        Set<ObjectId> check = new HashSet<>();
        for (ViewFieldChange change : changes) {
            check.addAll(Arrays.asList(change.getModelPathInstanceIds()));
        }

        Set<ObjectId> existence = existenceStore.getExistence(tenantIdAndCentricId.getTenantId(), check);

        List<ViewFieldChange> filtered = new ArrayList<>();
        for (ViewFieldChange fieldChange : changes) {
            ObjectId[] modelPathInstanceIds = fieldChange.getModelPathInstanceIds();

            if (fieldChange.getType() == ViewFieldChange.ViewFieldChangeType.remove
                    || existence.containsAll(Arrays.asList(modelPathInstanceIds))) {
                filtered.add(fieldChange);
            } else {
                if (LOG.isDebugEnabled()) {
                    StringBuilder msg = new StringBuilder("Ignoring ViewFieldChange: ").append(fieldChange).
                        append(" due to nonexistent objects. ");

                    String sep = "";
                    for (ObjectId objectId : modelPathInstanceIds) {
                        msg.append(sep);
                        msg.append(objectId).append(existence.contains(objectId) ? " exists" : " does not exist");
                        sep = ",";
                    }

                    LOG.debug(msg.toString());

                }
            }
        }
        commitChange.commitChange(tenantIdAndCentricId, filtered);
    }

}

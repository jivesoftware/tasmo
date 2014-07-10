package com.jivesoftware.os.tasmo.lib.read;

import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistenceCommitChange;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.model.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateReadTraversal;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
public class ReadMaterializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ReferenceTraverser referenceTraverser;
    private final FieldValueReader fieldValueReader;
    private final ConcurrencyStore concurrencyStore;
    private final TasmoViewModel tasmoViewModel;

    public ReadMaterializer(ReferenceTraverser referenceTraverser,
        FieldValueReader fieldValueReader,
        ConcurrencyStore concurrencyStore,
        TasmoViewModel tasmoViewModel) {

        this.referenceTraverser = referenceTraverser;
        this.fieldValueReader = fieldValueReader;
        this.concurrencyStore = concurrencyStore;
        this.tasmoViewModel = tasmoViewModel;
    }

    public Map<ViewDescriptor, List<ViewFieldChange>> readMaterialize(List<ViewDescriptor> requests) throws IOException {

        List<CommitChangeCollector> viewCollectors = buildViewCollectors(requests);
        try {
            for (CommitChangeCollector viewCollector : viewCollectors) {
                viewCollector.readMaterializeView();
            }
        } catch (Exception ex) {
            throw new IOException("Failed while read materializing view:" + requests, ex);
        }

        Map<ViewDescriptor, List<ViewFieldChange>> changes = new HashMap<>();
        for (CommitChangeCollector viewCollector : viewCollectors) {
            changes.put(viewCollector.getViewDescriptor(), viewCollector.getChanges());
        }
        return Collections.unmodifiableMap(changes);
    }

    private List<CommitChangeCollector> buildViewCollectors(List<ViewDescriptor> viewDescriptors) {

        List<CommitChangeCollector> collectors = new ArrayList<>();
        for (ViewDescriptor viewDescriptor : viewDescriptors) {
            CommitChangeCollector commitChangeCollector = new CommitChangeCollector(viewDescriptor);
            collectors.add(commitChangeCollector);
        }
        return collectors;
    }

    class CommitChangeCollector implements CommitChange {

        private final ViewDescriptor viewDescriptor;
        private final List<ViewFieldChange> changes = new ArrayList<>();

        CommitChangeCollector(ViewDescriptor viewDescriptor) {
            this.viewDescriptor = viewDescriptor;
        }

        public ViewDescriptor getViewDescriptor() {
            return viewDescriptor;
        }

        public List<ViewFieldChange> getChanges() {
            return Collections.unmodifiableList(changes);
        }

        @Override
        public void commitChange(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
            this.changes.addAll(changes);
        }

        private void readMaterializeView() throws Exception {
            VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(viewDescriptor.getTenantId());
            InitiateReadTraversal initiateTraversal = model.getReadTraversers().get(viewDescriptor.getViewId().getClassName());
            while (true) {
                try {
                    CommitChange commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, this);
                    initiateTraversal.read(referenceTraverser,
                        fieldValueReader,
                        new TenantIdAndCentricId(viewDescriptor.getTenantId(), viewDescriptor.getUserId()),
                        viewDescriptor.getViewId(),
                        commitChange);
                    return;
                } catch (Exception x) {
                    Throwable t = x;
                    while (t != null) {
                        if (t instanceof CommitChangeException) {
                            break;
                        }
                        t = t.getCause();
                    }
                    if (t instanceof CommitChangeException) {
                        // TODO add retry count and retry timeout!!
                        LOG.warn("Read reading viewDescriptor:" + viewDescriptor + " because it was modified while being read.");
                    } else {
                        throw x;
                    }
                }
            }
        }
    }
}

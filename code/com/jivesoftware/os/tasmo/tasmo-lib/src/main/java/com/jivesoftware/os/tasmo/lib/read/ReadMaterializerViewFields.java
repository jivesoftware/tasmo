package com.jivesoftware.os.tasmo.lib.read;

import com.google.common.util.concurrent.ListeningExecutorService;
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
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author jonathan.colt
 */
public class ReadMaterializerViewFields {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ReferenceTraverser referenceTraverser;
    private final FieldValueReader fieldValueReader;
    private final ConcurrencyStore concurrencyStore;
    private final TasmoViewModel tasmoViewModel;
    private final ListeningExecutorService processViewRequests;

    public ReadMaterializerViewFields(ReferenceTraverser referenceTraverser,
            FieldValueReader fieldValueReader,
            ConcurrencyStore concurrencyStore,
            TasmoViewModel tasmoViewModel,
            ListeningExecutorService processViewRequests) {

        this.referenceTraverser = referenceTraverser;
        this.fieldValueReader = fieldValueReader;
        this.concurrencyStore = concurrencyStore;
        this.tasmoViewModel = tasmoViewModel;
        this.processViewRequests = processViewRequests;
    }

    public Map<ViewDescriptor, ViewFieldsResponse> readMaterialize(List<ViewDescriptor> requests) throws IOException {

        try {
            final Map<ViewDescriptor, ViewFieldsResponse> changes = new ConcurrentHashMap<>();
            final CountDownLatch latch = new CountDownLatch(requests.size());
            for (final ViewDescriptor viewDescriptor : requests) {
                processViewRequests.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            CommitChangeCollector commitChangeCollector = new CommitChangeCollector(viewDescriptor);
                            commitChangeCollector.readMaterializeView();
                            changes.put(commitChangeCollector.getViewDescriptor(), new ViewFieldsResponse(commitChangeCollector.getChanges()));
                        } catch (Exception ex) {
                            LOG.warn("Failed while read materializing:" + viewDescriptor, ex);
                            changes.put(viewDescriptor, new ViewFieldsResponse(ex));
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            latch.await();
            return changes;
        } catch (Exception x) {
            throw new IOException("Failed while read materializing:" + requests, x);
        }
    }

    class CommitChangeCollector implements CommitChange {

        private final ViewDescriptor viewDescriptor;
        private final List<ViewField> changes = new ArrayList<>();

        CommitChangeCollector(ViewDescriptor viewDescriptor) {
            this.viewDescriptor = viewDescriptor;
        }

        public ViewDescriptor getViewDescriptor() {
            return viewDescriptor;
        }

        public List<ViewField> getChanges() {
            return changes;
        }

        @Override
        public void commitChange(WrittenEventContext batchContext,
                TenantIdAndCentricId tenantIdAndCentricId, List<ViewField> changes) throws CommitChangeException {
            this.changes.addAll(changes);
        }

        private void readMaterializeView() throws Exception {
            VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(viewDescriptor.getTenantId());
            Map<String, InitiateReadTraversal> readTraversers = model.getReadTraversers();
            String viewClassName = viewDescriptor.getViewId().getClassName();
            InitiateReadTraversal initiateTraversal = readTraversers.get(viewClassName);
            if (initiateTraversal == null) {
                throw new RuntimeException("No read traversal declared for viewClassName:" + viewClassName + ". Check your models.");
            } else {
                while (true) {
                    try {
                        CommitChange commitChange = new ConcurrencyAndExistenceCommitChange(concurrencyStore, this);
                        initiateTraversal.read(referenceTraverser,
                                fieldValueReader,
                                viewDescriptor.getTenantId(),
                                viewDescriptor.getActorId(),
                                viewDescriptor.getUserId(),
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
}

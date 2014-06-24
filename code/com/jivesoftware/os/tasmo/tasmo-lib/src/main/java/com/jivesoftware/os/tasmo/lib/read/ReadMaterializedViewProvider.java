package com.jivesoftware.os.tasmo.lib.read;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.VersionedTasmoViewModel;
import com.jivesoftware.os.tasmo.lib.process.WrittenEventContext;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateReadTraversal;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewFieldChange;
import com.jivesoftware.os.tasmo.lib.write.read.FieldValueReader;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.reference.lib.traverser.ReferenceTraverser;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReadMaterializer;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.ViewFieldsCollector;
import com.jivesoftware.os.tasmo.view.reader.service.ViewFormatter;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionCheckResult;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 *
 */
public class ReadMaterializedViewProvider<V> implements ViewReadMaterializer<V> {

    private static final String VIEW_READ_LATENCY = "view>materialize>read>latency";
    private static final String VIEW_PERMISSIONS_LATENCY = "view>materialize>permissions>latency";
    private static final String VIEW_MERGE_LATENCY = "view>materialize>merge>latency";
    private static final String VIEW_READ_VIEW_COUNT = "view>materialize>read>view>count";
    private static final String VIEW_READ_CALL_COUNT = "view>materialize>read>call>count";

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewPermissionChecker viewPermissionChecker;
    private final ReferenceTraverser referenceTraverser;
    private final FieldValueReader fieldValueReader;
    private final TasmoViewModel tasmoViewModel;
    private final ViewFormatter<V> viewFormatter;
    private final JsonViewMerger merger;

    public ReadMaterializedViewProvider(ViewPermissionChecker viewPermissionChecker,
        ReferenceTraverser referenceTraverser,
        FieldValueReader fieldValueReader,
        TasmoViewModel tasmoViewModel,
        ViewFormatter<V> viewFormatter,
        JsonViewMerger merger,
        long viewMaxSizeInBytes) {
        this.viewPermissionChecker = viewPermissionChecker;
        this.referenceTraverser = referenceTraverser;
        this.fieldValueReader = fieldValueReader;
        this.tasmoViewModel = tasmoViewModel;
        this.viewFormatter = viewFormatter;
        this.merger = merger;
    }

    @Override
    public V readMaterializeView(ViewDescriptor request) throws IOException {
        List<V> views = readMaterializeViews(ImmutableList.of(request));
        return views.isEmpty() ? viewFormatter.emptyView() : views.get(0);
    }

    @Override
    public List<V> readMaterializeViews(List<ViewDescriptor> request) throws IOException {
        LOG.inc(VIEW_READ_CALL_COUNT);
        Preconditions.checkArgument(request != null);
        ConcurrentMap<TenantAndActor, Set<Id>> permisionCheckTheseIds = new ConcurrentHashMap<>();
        List<CommitChangeCollector<V>> viewCollectors = buildViewCollectors(request, permisionCheckTheseIds);

        LOG.startTimer(VIEW_READ_LATENCY);
        try {
            for (CommitChangeCollector viewCollector : viewCollectors) {
                viewCollector.readMaterializeView();
            }
        } catch (Exception ex) {
            throw new IOException("Failed while read materializing view:"+request, ex);
        } finally {
            LOG.stopTimer(VIEW_READ_LATENCY);
        }

        Map<TenantAndActor, Set<Id>> canViewTheseIds;
        LOG.startTimer(VIEW_PERMISSIONS_LATENCY);
        try {
            canViewTheseIds = checkPermissions(permisionCheckTheseIds);
        } finally {
            LOG.stopTimer(VIEW_PERMISSIONS_LATENCY);
        }

        LOG.startTimer(VIEW_MERGE_LATENCY);
        try {
            List<V> views = new ArrayList<>();
            for (CommitChangeCollector<V> viewCollector : viewCollectors) {
                ViewDescriptor viewDescriptor = viewCollector.getViewDescriptor();
                Set<Id> viewableIds = canViewTheseIds.get(new TenantAndActor(viewDescriptor.getTenantId(), viewDescriptor.getActorId()));
                views.add(viewCollector.getView(viewableIds));
            }
            LOG.inc(VIEW_READ_VIEW_COUNT, views.size());
            return views;
        } catch (Exception ex) {
            LOG.error("Failed while loading {}", request);
            throw new IOException("Failed to load for the following reason.", ex);
        } finally {
            LOG.stopTimer(VIEW_MERGE_LATENCY);
        }
    }

    Map<TenantAndActor, Set<Id>> checkPermissions(Map<TenantAndActor, Set<Id>> permissionCheckTheseIds) {
        Map<TenantAndActor, Set<Id>> canViewTheseIds = new HashMap<>();
        for (TenantAndActor tenantAndActor : permissionCheckTheseIds.keySet()) { // 1 permissions check call per tenant and actor id tuple.
            Collection<Id> ids = permissionCheckTheseIds.get(tenantAndActor);
            ViewPermissionCheckResult checkResult = viewPermissionChecker.check(tenantAndActor.tenantId,
                tenantAndActor.actorId, new HashSet<>(ids));
            canViewTheseIds.put(tenantAndActor, Sets.union(checkResult.allowed(), checkResult.unknown())); // For now... TODO
        }
        return canViewTheseIds;
    }

    private List<CommitChangeCollector<V>> buildViewCollectors(List<ViewDescriptor> viewDescriptors,
        ConcurrentMap<TenantAndActor, Set<Id>> permisionCheckTheseIds) {

        List<CommitChangeCollector<V>> collectors = new ArrayList<>();
        for (ViewDescriptor viewDescriptor : viewDescriptors) {
            TenantAndActor tenantAndActor = new TenantAndActor(viewDescriptor.getTenantId(), viewDescriptor.getActorId());
            Set<Id> checkTheseIds = permisionCheckTheseIds.get(tenantAndActor);
            if (checkTheseIds == null) {
                checkTheseIds = new HashSet<>();
                Set<Id> had = permisionCheckTheseIds.putIfAbsent(tenantAndActor, checkTheseIds);
                if (had != null) {
                    checkTheseIds = had;
                }
            }

            ViewFieldsCollector viewFieldsCollector = new ViewFieldsCollector(merger, 1024 * 1024 * 10);
            CommitChangeCollector<V> commitChangeCollector = new CommitChangeCollector<>(viewDescriptor, viewFieldsCollector, viewFormatter, checkTheseIds);
            collectors.add(commitChangeCollector);

        }

        return collectors;
    }

    class CommitChangeCollector<VV> implements CommitChange {

        private final ViewDescriptor viewDescriptor;
        private final ViewFieldsCollector viewFieldsCollector;
        private final ViewFormatter<VV> viewFormatter;
        private final Set<Id> permisionCheckTheseIds;

        public CommitChangeCollector(ViewDescriptor viewDescriptor,
            ViewFieldsCollector viewFieldsCollector,
            ViewFormatter<VV> viewFormatter,
            Set<Id> permisionCheckTheseIds) {
            this.viewDescriptor = viewDescriptor;
            this.viewFieldsCollector = viewFieldsCollector;
            this.viewFormatter = viewFormatter;
            this.permisionCheckTheseIds = permisionCheckTheseIds;
        }

        public ViewDescriptor getViewDescriptor() {
            return viewDescriptor;
        }

        @Override
        public void commitChange(WrittenEventContext batchContext,
            TenantIdAndCentricId tenantIdAndCentricId, List<ViewFieldChange> changes) throws CommitChangeException {
            try {
                System.out.println("changes:" + changes);
                for (ViewFieldChange change : changes) {
                    ModelPath modelPath = change.getModelPath();
                    Id[] modelPathIds = ids(change.getModelPathInstanceIds());
                    permisionCheckTheseIds.addAll(Arrays.asList(modelPathIds));
                    String[] viewPathClasses = classes(change.getModelPathInstanceIds());
                    ViewValue viewValue = new ViewValue(change.getModelPathTimestamps(), change.getValue());
                    Long timestamp = change.getTimestamp();
                    viewFieldsCollector.add(viewDescriptor, modelPath, modelPathIds, viewPathClasses, viewValue, timestamp);
                }
            } catch (IOException x) {
                throw new CommitChangeException("Failed to collect fields for "+tenantIdAndCentricId, x);
            }
        }

        public VV getView(Set<Id> canViewTheseIds) throws Exception {
            ViewResponse view = viewFieldsCollector.getView(canViewTheseIds);
            TenantIdAndCentricId tenantIdAndCentricId = viewDescriptor.getTenantIdAndCentricId();
            return viewFormatter.getView(tenantIdAndCentricId, viewDescriptor.getViewId(), view);
        }

        private Id[] ids(PathId[] pathIds) {
            Id[] ids = new Id[pathIds.length];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = pathIds[i].getObjectId().getId();
            }
            return ids;
        }

        private String[] classes(PathId[] pathIds) {
            String[] classes = new String[pathIds.length];
            for (int i = 0; i < classes.length; i++) {
                classes[i] = pathIds[i].getObjectId().getClassName();
            }
            return classes;
        }

        private void readMaterializeView() throws Exception {
            VersionedTasmoViewModel model = tasmoViewModel.getVersionedTasmoViewModel(viewDescriptor.getTenantId());
            InitiateReadTraversal initiateTraversal = model.getReadTraversers().get(viewDescriptor.getViewId().getClassName());
            initiateTraversal.read(referenceTraverser,
            fieldValueReader,
            new TenantIdAndCentricId(viewDescriptor.getTenantId(), viewDescriptor.getUserId()), // TODO resolve ?? userId or actorId?
            viewDescriptor.getViewId(),
            this);
        }
    }
}

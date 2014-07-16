package com.jivesoftware.os.tasmo.lib.read;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
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
public class JITReadMaterializeViewProvider<V> implements ViewReadMaterializer<V> {

    private static final String VIEW_READ_LATENCY = "view>materialize>read>latency";
    private static final String VIEW_PERMISSIONS_LATENCY = "view>materialize>permissions>latency";
    private static final String VIEW_MERGE_LATENCY = "view>materialize>merge>latency";
    private static final String VIEW_READ_VIEW_COUNT = "view>materialize>read>view>count";
    private static final String VIEW_READ_CALL_COUNT = "view>materialize>read>call>count";

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewPermissionChecker viewPermissionChecker;
    private final ReadMaterializerViewFields readMaterializer;
    private final ViewFormatter<V> viewFormatter;
    private final JsonViewMerger merger;
    private final Optional<CommitChange> commitChangeVistor;

    public JITReadMaterializeViewProvider(ViewPermissionChecker viewPermissionChecker,
        ReadMaterializerViewFields readMaterializer,
        ViewFormatter<V> viewFormatter,
        JsonViewMerger merger,
        Optional<CommitChange> commitChangeVistor,
        long viewMaxSizeInBytes) {

        this.viewPermissionChecker = viewPermissionChecker;
        this.readMaterializer = readMaterializer;
        this.viewFormatter = viewFormatter;
        this.merger = merger;
        this.commitChangeVistor = commitChangeVistor;
    }

    @Override
    public V readMaterializeView(ViewDescriptor request) throws IOException {
        List<V> views = readMaterializeViews(ImmutableList.of(request));
        return views.isEmpty() ? viewFormatter.emptyView() : views.get(0);
    }

    @Override
    public List<V> readMaterializeViews(List<ViewDescriptor> viewDescriptors) throws IOException {
        LOG.inc(VIEW_READ_CALL_COUNT);
        Preconditions.checkArgument(viewDescriptors != null);
        ConcurrentMap<TenantAndActor, Set<Id>> permisionCheckTheseIds = new ConcurrentHashMap<>();
        Map<ViewDescriptor, ViewFieldsCollector> collectors = new HashMap<>();

        LOG.startTimer(VIEW_READ_LATENCY);
        try {

            Map<ViewDescriptor, ViewFieldsResponse> readMaterialized = readMaterializer.readMaterialize(viewDescriptors);
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

                ViewFieldsResponse viewFieldsResponse = readMaterialized.get(viewDescriptor);
                if (viewFieldsResponse.isOk()) {
                    try {
                        ViewFieldsCollector viewFieldsCollector = new ViewFieldsCollector(merger, 1_024 * 1_024 * 10);
                        for (ViewField viewField : viewFieldsResponse.getViewFields()) {
                            if (viewField.getType() == ViewField.ViewFieldChangeType.add) {
                                ModelPath modelPath = viewField.getModelPath();
                                Id[] modelPathIds = ids(viewField.getModelPathInstanceIds());
                                checkTheseIds.addAll(Arrays.asList(modelPathIds));
                                String[] viewPathClasses = classes(viewField.getModelPathInstanceIds());
                                ViewValue viewValue = new ViewValue(viewField.getModelPathTimestamps(), viewField.getValue());
                                Long timestamp = viewField.getTimestamp();
                                viewFieldsCollector.add(viewDescriptor, modelPath, modelPathIds, viewPathClasses, viewValue, timestamp);
                            }
                        }
                        collectors.put(viewDescriptor, viewFieldsCollector);
                    } catch (IOException x) {
                        throw new CommitChangeException("Failed to collect fields for " + viewDescriptor, x);
                    }

                    if (commitChangeVistor.isPresent()) {
                        try {
                            commitChangeVistor.get().commitChange(null, viewDescriptor.getTenantIdAndCentricId(), viewFieldsResponse.getViewFields());
                        } catch (Exception x) {
                            LOG.warn("Commit Change Collector's vistor encouter the following issue.", x);
                        }
                    }
                } else {
                    LOG.error("Not returning view for " + viewDescriptor + " because of ", viewFieldsResponse.getException());
                }
            }
        } catch (Exception ex) {
            throw new IOException("Failed while read materializing view:" + viewDescriptors, ex);
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
            for (ViewDescriptor viewDescriptor : viewDescriptors) {
                ViewFieldsCollector viewFieldsCollector = collectors.get(viewDescriptor);
                Set<Id> viewableIds = canViewTheseIds.get(new TenantAndActor(viewDescriptor.getTenantId(), viewDescriptor.getActorId()));
                ViewResponse viewResponse = viewFieldsCollector.getView(viewableIds);
                TenantIdAndCentricId tenantIdAndCentricId = viewDescriptor.getTenantIdAndCentricId();
                V view = viewFormatter.getView(tenantIdAndCentricId, viewDescriptor.getViewId(), viewResponse);
                views.add(view);
            }
            LOG.inc(VIEW_READ_VIEW_COUNT, views.size());
            return views;
        } catch (Exception ex) {
            LOG.error("Failed while loading {}", viewDescriptors);
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

}

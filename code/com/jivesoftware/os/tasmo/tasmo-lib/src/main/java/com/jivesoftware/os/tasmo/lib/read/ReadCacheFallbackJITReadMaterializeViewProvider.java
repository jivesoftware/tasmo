package com.jivesoftware.os.tasmo.lib.read;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.modifier.ModifierStore;
import com.jivesoftware.os.tasmo.lib.write.CommitChangeException;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.TenantAndActor;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReadMaterializer;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.ViewFieldsCollector;
import com.jivesoftware.os.tasmo.view.reader.service.ViewFormatter;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionCheckResult;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import com.jivesoftware.os.tasmo.id.ViewValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author jonathan.colt
 */
public class ReadCacheFallbackJITReadMaterializeViewProvider<V> implements ViewReadMaterializer<V> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ReadCachedViewFields readCachedViewFields;
    private final ReadMaterializerViewFields readMaterializerViewFields;
    private final Optional<ModifierStore> modifierStore;
    private final ViewPermissionChecker viewPermissionChecker;
    private final ViewFormatter<V> viewFormatter;
    private final JsonViewMerger merger;

    public ReadCacheFallbackJITReadMaterializeViewProvider(ReadCachedViewFields readCachedViewFields,
        ReadMaterializerViewFields readMaterializerViewFields,
        Optional<ModifierStore> modifierStore,
        ViewPermissionChecker viewPermissionChecker,
        ViewFormatter<V> viewFormatter,
        JsonViewMerger merger) {

        this.readCachedViewFields = readCachedViewFields;
        this.readMaterializerViewFields = readMaterializerViewFields;
        this.modifierStore = modifierStore;
        this.viewPermissionChecker = viewPermissionChecker;
        this.viewFormatter = viewFormatter;
        this.merger = merger;
    }

    @Override
    public V readMaterializeView(ViewDescriptor request) throws IOException {
        List<V> views = readMaterializeViews(ImmutableList.of(request));
        return views.isEmpty() ? viewFormatter.emptyView() : views.get(0);
    }

    @Override
    public List<V> readMaterializeViews(List<ViewDescriptor> viewDescriptors) throws IOException {
        Preconditions.checkArgument(viewDescriptors != null);

        Map<ViewDescriptor, ViewFieldsResponse> cachedViews = readCachedViewFields.readViews(viewDescriptors);
        List<ViewDescriptor> needToReadMaterializeTheseViews = new ArrayList<>();
        for (ViewDescriptor viewDescriptor : viewDescriptors) {
            ViewFieldsResponse viewFieldsResponse = cachedViews.get(viewDescriptor);
            if (!viewFieldsResponse.isOk() || viewFieldsResponse.getViewFields().isEmpty()) {
                needToReadMaterializeTheseViews.add(viewDescriptor);
            } else {
                if (modifierStore.isPresent()) {
                    Map<ObjectId, Long> implicated = new HashMap<>();
                    for (ViewField viewField : viewFieldsResponse.getViewFields()) {
                        PathId[] modelPathInstanceIds = viewField.getModelPathInstanceIds();
                        for (PathId pathId : modelPathInstanceIds) {
                            implicated.put(pathId.getObjectId(), pathId.getTimestamp());
                        }
                    }
                    List<PathId> modified = modifierStore.get().get(viewDescriptor.getTenantId(), viewDescriptor.getUserId(), implicated.keySet());
                    for (PathId pathId : modified) {
                        if (implicated.get(pathId.getObjectId()) > pathId.getTimestamp()) {
                            needToReadMaterializeTheseViews.add(viewDescriptor);
                        }
                    }
                }
            }
        }
        Map<ViewDescriptor, ViewFieldsResponse> jitReadMateralizeViewField;
        if (!needToReadMaterializeTheseViews.isEmpty()) {
            jitReadMateralizeViewField = readMaterializerViewFields.readMaterialize(needToReadMaterializeTheseViews);
            // TODO? could add a hook that updates the cache. This creates a single writter problem.
        } else {
            jitReadMateralizeViewField = Collections.emptyMap();
        }

        ConcurrentMap<TenantAndActor, Set<Id>> permisionCheckTheseIds = new ConcurrentHashMap<>();
        Map<ViewDescriptor, ViewFieldsCollector> collectors = new HashMap<>();
        try {

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

                ViewFieldsResponse viewFieldsResponse = jitReadMateralizeViewField.get(viewDescriptor);
                if (viewFieldsResponse == null) {
                    viewFieldsResponse = cachedViews.get(viewDescriptor);
                    if (viewFieldsResponse.isOk() && !viewFieldsResponse.getViewFields().isEmpty()) {
                        LOG.inc("viewCache>hits");
                    }
                } else {
                    LOG.inc("jitm>hits");
                }
                try {
                    ViewFieldsCollector viewFieldsCollector = new ViewFieldsCollector(merger, 1_024 * 1_024 * 10);
                    for (ViewField change : viewFieldsResponse.getViewFields()) {
                        if (change.getType() == ViewField.ViewFieldChangeType.add) {
                            ModelPath modelPath = change.getModelPath();
                            Id[] modelPathIds = ids(change.getModelPathInstanceIds());
                            checkTheseIds.addAll(Arrays.asList(modelPathIds));
                            String[] viewPathClasses = classes(change.getModelPathInstanceIds());
                            ViewValue viewValue = new ViewValue(change.getModelPathTimestamps(), change.getValue());
                            Long timestamp = change.getTimestamp();
                            viewFieldsCollector.add(viewDescriptor, modelPath, modelPathIds, viewPathClasses, viewValue, timestamp);
                        }
                    }
                    collectors.put(viewDescriptor, viewFieldsCollector);
                } catch (IOException x) {
                    throw new CommitChangeException("Failed to collect fields for " + viewDescriptor, x);
                }
            }
        } catch (Exception ex) {
            throw new IOException("Failed while read materializing view:" + viewDescriptors, ex);
        }

        Map<TenantAndActor, Set<Id>> canViewTheseIds;
        canViewTheseIds = checkPermissions(permisionCheckTheseIds);
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
            return views;
        } catch (Exception ex) {
            LOG.error("Failed while loading {}", viewDescriptors);
            throw new IOException("Failed to load for the following reason.", ex);
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

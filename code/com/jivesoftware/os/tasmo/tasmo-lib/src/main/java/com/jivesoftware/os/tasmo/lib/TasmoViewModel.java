/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.tasmo.lib;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.WrittenInstanceHelper;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateBackRefTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateBackrefRemovalTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateExistenceTransitionTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateRefRemovalTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateRefTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraverserKey;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateValueTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverser;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverserConfig;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraversersFactory;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TasmoViewModel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final WrittenEventProvider writtenEventProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;
    private final ConcurrencyStore concurrencyStore;
    private final ReferenceStore referenceStore;
    private final EventValueStore eventValueStore;
    private final CommitChange changeWriter;
    private final WrittenInstanceHelper writtenInstanceHelper = new WrittenInstanceHelper();

    public TasmoViewModel(
            TenantId masterTenantId,
            ViewsProvider viewsProvider,
            WrittenEventProvider writtenEventProvider,
            ConcurrencyStore concurrencyStore,
            ReferenceStore referenceStore,
            EventValueStore eventValueStore,
            CommitChange changeWriter) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.writtenEventProvider = writtenEventProvider;
        this.concurrencyStore = concurrencyStore;
        this.referenceStore = referenceStore;
        this.eventValueStore = eventValueStore;
        this.changeWriter = changeWriter;
        this.versionedViewModels = new ConcurrentHashMap<>();
    }

    VersionedTasmoViewModel getVersionedTasmoViewModel(TenantId tenantId) throws Exception {
        if (!versionedViewModels.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedTasmoViewModel viewsModel = versionedViewModels.get(tenantId);
        if (viewsModel == null || viewsModel.getDispatchers() == null) {
            if (!tenantId.equals(masterTenantId)) {
                viewsModel = versionedViewModels.get(masterTenantId);
            }
        }

        return viewsModel;
    }

    public void reloadModels() {
        for (TenantId tenantId : versionedViewModels.keySet()) {
            loadModel(tenantId);
        }
    }

    synchronized public void loadModel(TenantId tenantId) {
        ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
        if (currentVersion == ChainedVersion.NULL) {
            versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, null));
        } else {
            VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
            if (currentVersionedViewsModel == null
                    || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                if (views != null) {
                    ListMultimap<String, InitiateTraversal> dispatchers = bindModelPaths(views);
                    versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(), dispatchers));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }

    }

    private ListMultimap<String, InitiateTraversal> bindModelPaths(Views views) throws IllegalArgumentException {

        Map<String, PathTraversersFactory> allFieldProcessorFactories = Maps.newHashMap();

        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupSteps = new HashMap<>();
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupIdCentricSteps = new HashMap<>();

        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();
            boolean isNotificationRequired = viewBinding.isNotificationRequired();

            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> accumlulate = groupSteps;
            if (idCentric) {
                accumlulate = groupIdCentricSteps;
            }

            PathTraverserConfig executableStepConfig = new PathTraverserConfig(
                    writtenEventProvider,
                    changeWriter,
                    viewIdFieldName,
                    idCentric,
                    isNotificationRequired);

            for (ModelPath modelPath : viewBinding.getModelPaths()) {

                String factoryKey = viewBinding.getViewClassName() + "_" + modelPath.getId();
                if (allFieldProcessorFactories.containsKey(factoryKey)) {
                    throw new IllegalArgumentException("you have already created this binding:" + factoryKey);
                }

                PathTraversersFactory fieldProcessorFactory = new PathTraversersFactory(viewClassName, modelPath, eventValueStore, referenceStore);

                LOG.info("Bind:{}", factoryKey);
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

                List<PathTraverser> executableSteps = fieldProcessorFactory.buildPathTraversers(executableStepConfig);
                groupExecutableStepsByClass(accumlulate, executableSteps);

                PathTraverser initialBackRefStep = fieldProcessorFactory.buildInitialBackrefStep(executableStepConfig);
                if (initialBackRefStep != null) {
                    groupExecutableStepsByClass(accumlulate, Arrays.asList(initialBackRefStep));
                }
            }

        }
        ListMultimap<String, InitiateTraversal> all = ArrayListMultimap.create();
        buildInitialStepDispatchers(groupSteps, false, all);
        buildInitialStepDispatchers(groupIdCentricSteps, true, all);

        return all;
    }

    private void buildInitialStepDispatchers(Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupSteps,
            boolean idCentric,
            ListMultimap<String, InitiateTraversal> all) {

        for (String className : groupSteps.keySet()) {
            Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> typedSteps = groupSteps.get(className);
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefs = typedSteps.get(ModelPathStepType.backRefs);
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> counts = typedSteps.get(ModelPathStepType.count);
            ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefsAndCounts = ArrayListMultimap.create();
            if (backRefs != null) {
                backRefsAndCounts.putAll(backRefs);
            }
            if (counts != null) {
                backRefsAndCounts.putAll(counts);
            }
            InitiateTraversal initialStepDispatcher = new InitiateTraversal(className,
                    new InitiateValueTraversal(eventValueStore, typedSteps.get(ModelPathStepType.value), idCentric),
                    new InitiateRefTraversal(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.ref), idCentric),
                    new InitiateRefTraversal(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.refs), idCentric),
                    new InitiateBackRefTraversal(writtenInstanceHelper, referenceStore, backRefsAndCounts, idCentric),
                    new InitiateBackRefTraversal(writtenInstanceHelper, referenceStore, typedSteps.get(ModelPathStepType.latest_backRef), idCentric),
                    new InitiateRefRemovalTraversal(referenceStore, typedSteps.get(ModelPathStepType.ref), idCentric),
                    new InitiateRefRemovalTraversal(referenceStore, typedSteps.get(ModelPathStepType.refs), idCentric),
                    new InitiateBackrefRemovalTraversal(referenceStore, typedSteps.get(ModelPathStepType.latest_backRef), idCentric),
                    new InitiateBackrefRemovalTraversal(referenceStore, backRefs, idCentric),
                    new InitiateBackrefRemovalTraversal(referenceStore, counts, idCentric),
                    new InitiateExistenceTransitionTraversal(typedSteps, idCentric));
            all.put(className, initialStepDispatcher);
        }
    }

    private void groupExecutableStepsByClass(
            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupedPathTraversers,
            List<PathTraverser> pathTraverers) {

        for (PathTraverser pathTraverser : pathTraverers) {
            for (String className : pathTraverser.getInitialClassNames()) {
                Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> typedSteps = groupedPathTraversers.get(className);
                if (typedSteps == null) {
                    typedSteps = Maps.newHashMap();
                    groupedPathTraversers.put(className, typedSteps);
                }
                ModelPathStepType stepType = pathTraverser.getInitialModelPathStepType();
                ArrayListMultimap<InitiateTraverserKey, PathTraverser> steps = typedSteps.get(stepType);
                if (steps == null) {
                    steps = ArrayListMultimap.create();
                    typedSteps.put(stepType, steps);
                }

                String refFieldName = pathTraverser.getRefFieldName();
                if (refFieldName != null) {
                    steps.put(new InitiateTraverserKey(refFieldName, refFieldName), pathTraverser);
                }

                for (String fieldName : pathTraverser.getInitialFieldNames()) {
                    steps.put(new InitiateTraverserKey(fieldName, refFieldName), pathTraverser);
                }
            }
        }
    }
}

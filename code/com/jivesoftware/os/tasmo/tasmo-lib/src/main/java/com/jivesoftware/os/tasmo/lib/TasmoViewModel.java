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
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.events.EventValueStore;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateRefTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraverserKey;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateValueTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverser;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverserConfig;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraversersFactory;
import com.jivesoftware.os.tasmo.lib.process.traversal.StepTraverser;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.process.WrittenEventProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
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
            versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, null, null));
        } else {
            VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
            if (currentVersionedViewsModel == null
                    || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                if (views != null) {
                    ListMultimap<String, InitiateTraversal> dispatchers = bindModelPaths(views);
                    ListMultimap<String, FieldNameAndType> eventModel = bindEventFieldTypes(views);
                    versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(), dispatchers, eventModel));
                } else {
                    LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                }
            } else {
                LOG.debug("Didn't reload because view model versions are equal.");
            }
        }

    }

    private ListMultimap<String, FieldNameAndType> bindEventFieldTypes(Views views) throws IllegalArgumentException {
        ListMultimap<String, FieldNameAndType> eventModel = ArrayListMultimap.create();
        for (ViewBinding viewBinding : views.getViewBindings()) {
            boolean idCentric = viewBinding.isIdCentric();
            for (ModelPath modelPath : viewBinding.getModelPaths()) {
                for (ModelPathStep modelPathStep : modelPath.getPathMembers()) {
                    String refFieldName = modelPathStep.getRefFieldName();
                    if (refFieldName != null) {
                        for (String className : modelPathStep.getOriginClassNames()) {
                            eventModel.put(className, new FieldNameAndType(refFieldName, ModelPathStepType.ref, idCentric));
                        }

                    } else {
                        for (String fieldName : modelPathStep.getFieldNames()) {
                            for (String className : modelPathStep.getOriginClassNames()) {
                                eventModel.put(className, new FieldNameAndType(fieldName, ModelPathStepType.value, idCentric));
                            }
                        }
                    }
                }
            }
        }
        return eventModel;
    }

    public static class FieldNameAndType {

        private final String fieldName;
        private final ModelPathStepType fieldType;
        private final boolean idCentric;

        public FieldNameAndType(String fieldName, ModelPathStepType fieldType, boolean idCentric) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.idCentric = idCentric;
        }

        public String getFieldName() {
            return fieldName;
        }

        public ModelPathStepType getFieldType() {
            return fieldType;
        }

        public boolean isIdCentric() {
            return idCentric;
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

            PathTraverserConfig pathTraverserConfig = new PathTraverserConfig(
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
                LOG.trace("MODELPATH " + modelPath);
                PathTraversersFactory fieldProcessorFactory = new PathTraversersFactory(viewClassName,
                        modelPath, eventValueStore, referenceStore);

                LOG.info("Bind:{}", factoryKey);
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

                List<PathTraverser> pathTraversers = fieldProcessorFactory.buildPathTraversers(pathTraverserConfig);
                groupPathTraverserByClass(accumlulate, pathTraversers);

                List<PathTraverser> initialBackRefStep = fieldProcessorFactory.buildBackPathTraversers(pathTraverserConfig);
                if (initialBackRefStep != null) {
                    groupPathTraverserByClass(accumlulate, initialBackRefStep);
                }
            }

        }
        ListMultimap<String, InitiateTraversal> all = ArrayListMultimap.create();
        all.putAll(buildInitialStepDispatchers(groupSteps));
        return all;
    }

    private ListMultimap<String, InitiateTraversal> buildInitialStepDispatchers(
            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupSteps) {

        ListMultimap<String, InitiateTraversal> all = ArrayListMultimap.create();
        for (String className : groupSteps.keySet()) {
            Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> typedSteps = groupSteps.get(className);

            ArrayListMultimap<InitiateTraverserKey, PathTraverser> refTraversers = ArrayListMultimap.create();
            if (typedSteps.get(ModelPathStepType.ref) != null) {
                refTraversers.putAll(typedSteps.get(ModelPathStepType.ref));
            }
            if (typedSteps.get(ModelPathStepType.refs) != null) {
                refTraversers.putAll(typedSteps.get(ModelPathStepType.refs));
            }

            ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers = ArrayListMultimap.create();
            if (typedSteps.get(ModelPathStepType.backRefs) != null) {
                backRefTraversers.putAll(typedSteps.get(ModelPathStepType.backRefs));
            }
            if (typedSteps.get(ModelPathStepType.latest_backRef) != null) {
                backRefTraversers.putAll(typedSteps.get(ModelPathStepType.latest_backRef));
            }
            if (typedSteps.get(ModelPathStepType.count) != null) {
                backRefTraversers.putAll(typedSteps.get(ModelPathStepType.count));
            }
            ConcurrencyChecker concurrencyChecker = new ConcurrencyChecker(concurrencyStore);

            InitiateTraversal initialStepDispatcher = new InitiateTraversal(className,
                    new InitiateValueTraversal(concurrencyChecker, eventValueStore, typedSteps.get(ModelPathStepType.value)),
                    new InitiateRefTraversal(concurrencyChecker, referenceStore, refTraversers, backRefTraversers));
            all.put(className, initialStepDispatcher);

            LOG.trace("---- Traversers for class:" + className);
            for (InitiateTraverserKey key : backRefTraversers.keySet()) {
                LOG.trace("-------- for key:" + key);

                for (PathTraverser pathTraverser : backRefTraversers.get(key)) {

                    for (StepTraverser stepTraverser : pathTraverser.getStepTraversers()) {
                        LOG.trace("------------ traverser:" + stepTraverser);
                    }
                }
            }
            LOG.trace("");

        }
        return all;
    }

    private void groupPathTraverserByClass(
            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>>> groupedPathTraversers,
            List<PathTraverser> pathTraverers) {

        for (PathTraverser pathTraverser : pathTraverers) {
            for (String className : pathTraverser.getInitialClassNames()) {
                LOG.trace("CLASSNAME:" + className);
                Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, PathTraverser>> typedSteps = groupedPathTraversers.get(className);
                if (typedSteps == null) {
                    typedSteps = Maps.newHashMap();
                    groupedPathTraversers.put(className, typedSteps);
                }
                ModelPathStepType stepType = pathTraverser.getInitialModelPathStepType();
                LOG.trace("STEPTYPE:" + stepType);
                ArrayListMultimap<InitiateTraverserKey, PathTraverser> steps = typedSteps.get(stepType);
                if (steps == null) {
                    steps = ArrayListMultimap.create();
                    typedSteps.put(stepType, steps);
                }

                String refFieldName = pathTraverser.getRefFieldName();
                if (refFieldName != null) {
                    LOG.trace("REFFIELDNAME:" + refFieldName);
                    steps.put(new InitiateTraverserKey(refFieldName, refFieldName), pathTraverser);
                    for (StepTraverser stepTraverser : pathTraverser.getStepTraversers()) {
                        LOG.trace("------------ STEP:" + stepTraverser);
                    }
                }

                for (String fieldName : pathTraverser.getInitialFieldNames()) {
                    LOG.trace("INITIALFIELDNAME:" + fieldName);
                    steps.put(new InitiateTraverserKey(fieldName, refFieldName), pathTraverser);
                    for (StepTraverser stepTraverser : pathTraverser.getStepTraversers()) {
                        LOG.trace("------------ STEP:" + stepTraverser);
                    }
                }
            }
            LOG.trace("------------------------------------------------");
        }
    }
}

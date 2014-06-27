package com.jivesoftware.os.tasmo.lib;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.event.api.ReservedFields;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateReadTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraversalContext;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateTraverserKey;
import com.jivesoftware.os.tasmo.lib.process.traversal.InitiateWriteTraversal;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathAtATimeStepStreamerFactory;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverser;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraverserKey;
import com.jivesoftware.os.tasmo.lib.process.traversal.PathTraversersFactory;
import com.jivesoftware.os.tasmo.lib.process.traversal.PrefixCollapsedStepStreamerFactory;
import com.jivesoftware.os.tasmo.lib.process.traversal.StepTraverser;
import com.jivesoftware.os.tasmo.lib.process.traversal.StepTree;
import com.jivesoftware.os.tasmo.lib.process.traversal.TraversablePath;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TasmoViewModel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final ViewPathKeyProvider viewPathKeyProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;
    private final ConcurrencyStore concurrencyStore;
    private final ReferenceStore referenceStore;
    private final StripingLocksProvider<TenantId> loadModelLocks = new StripingLocksProvider<>(1024);

    public TasmoViewModel(
        TenantId masterTenantId,
        ViewsProvider viewsProvider,
        ViewPathKeyProvider viewPathKeyProvider,
        ConcurrencyStore concurrencyStore,
        ReferenceStore referenceStore) {
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.viewPathKeyProvider = viewPathKeyProvider;
        this.concurrencyStore = concurrencyStore;
        this.referenceStore = referenceStore;
        this.versionedViewModels = new ConcurrentHashMap<>();
    }

    public VersionedTasmoViewModel getVersionedTasmoViewModel(TenantId tenantId) throws Exception {
        if (!versionedViewModels.containsKey(tenantId)) {
            loadModel(tenantId);
        }
        VersionedTasmoViewModel viewsModel = versionedViewModels.get(tenantId);
        if (viewsModel == null || viewsModel.getWriteTraversers() == null) {
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

    public void loadModel(TenantId tenantId) {
        synchronized (loadModelLocks.lock(tenantId)) {
            ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
            if (currentVersion == ChainedVersion.NULL) {
                versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, null, null, null, null));
            } else {
                VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
                if (currentVersionedViewsModel == null
                    || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                    Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                    if (views != null) {
                        Map<String, InitiateWriteTraversal> writeTraversal = bindModelPaths(views);
                        Map<String, InitiateReadTraversal> readTraversal = buildViewReaderTraveral(views);
                        SetMultimap<String, FieldNameAndType> eventModel = bindEventFieldTypes(views);
                        Set<String> notifiableViewClassNames = buildNotifiableViewClassNames(views);
                        versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(),
                            writeTraversal, readTraversal, eventModel, notifiableViewClassNames));
                    } else {
                        LOG.info("ViewsProvider failed to provide a 'Views' instance for tenantId:" + tenantId);
                    }
                } else {
                    LOG.debug("Didn't reload because view model versions are equal.");
                }
            }
        }

    }

    private Set<String> buildNotifiableViewClassNames(Views views) throws IllegalArgumentException {
        Set<String> notifiableViewClassNames = new HashSet<>();
        for (ViewBinding viewBinding : views.getViewBindings()) {
            if (viewBinding.isNotificationRequired()) {
                notifiableViewClassNames.add(viewBinding.getViewClassName());
            }
        }
        return notifiableViewClassNames;
    }

    private SetMultimap<String, FieldNameAndType> bindEventFieldTypes(Views views) throws IllegalArgumentException {
        SetMultimap<String, FieldNameAndType> eventModel = HashMultimap.create();
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

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("fieldName", fieldName)
                .add("fieldType", fieldType)
                .add("idCentric", idCentric)
                .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldNameAndType that = (FieldNameAndType) o;

            if (idCentric != that.idCentric) {
                return false;
            }
            if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
                return false;
            }
            if (fieldType != that.fieldType) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = fieldName != null ? fieldName.hashCode() : 0;
            result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
            result = 31 * result + (idCentric ? 1 : 0);
            return result;
        }
    }

    private Map<String, InitiateReadTraversal> buildViewReaderTraveral(Views views) throws IllegalArgumentException {

        Map<String, PathTraversersFactory> allFieldProcessorFactories = Maps.newHashMap();

        Map<String, InitiateReadTraversal> readTraversers = new HashMap<>();
        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();

            Set<String> rootingEventClassNames = new HashSet<>();
            List<PathAtATimeStepStreamerFactory> streamers = new ArrayList<>();
            List<ModelPath> modelPaths = viewBinding.getModelPaths();
            for (ModelPath modelPath : modelPaths) {
                assertNoInstanceIdBindings(viewBinding, modelPath);

                rootingEventClassNames.addAll(modelPath.getRootClassNames());

                String factoryKey = viewBinding.getViewClassName() + "_" + modelPath.getId();
                if (allFieldProcessorFactories.containsKey(factoryKey)) {
                    throw new IllegalArgumentException("you have already created this binding:" + factoryKey);
                }

                long modelPathHashcode = viewPathKeyProvider.modelPathHashcode(modelPath.getId());
                PathTraversersFactory fieldProcessorFactory = new PathTraversersFactory(viewClassName, modelPathHashcode, modelPath);
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

                List<StepTraverser> steps = fieldProcessorFactory.buildReadSteps(viewIdFieldName);
                streamers.add(new PathAtATimeStepStreamerFactory(steps));
            }

            readTraversers.put(viewClassName, new InitiateReadTraversal(rootingEventClassNames, streamers, modelPaths.size() + 1, idCentric));

        }
        return readTraversers;
    }

    List<PathTraverser> transformToPrefixCollapsedTree(List<TraversablePath> traversablePaths) {
        if (traversablePaths == null) {
            return null;
        }

        Map<PathTraverserKey, StepTree> subTrees = new ConcurrentHashMap<>();
        for (TraversablePath traversablePath : traversablePaths) {
            InitiateTraversalContext initialStepContext = traversablePath.getInitialStepContext();
            PathTraverserKey pathTraverserKey = new PathTraverserKey(initialStepContext.getInitialFieldNames(),
                initialStepContext.getPathIndex(),
                initialStepContext.getMembersSize());

            StepTree stepTree = subTrees.get(pathTraverserKey);
            if (stepTree == null) {
                stepTree = new StepTree();
                subTrees.put(pathTraverserKey, stepTree);
            }
            stepTree.add(traversablePath.getStepTraversers());
            stepTree.print();
        }

        List<PathTraverser> pathTraversers = new ArrayList<>();
        for (PathTraverserKey pathTraverserKey : subTrees.keySet()) {
            StepTree stepTree = subTrees.get(pathTraverserKey);
            pathTraversers.add(new PathTraverser(pathTraverserKey, new PrefixCollapsedStepStreamerFactory(stepTree)));
        }

        return pathTraversers;
    }

    private Map<String, InitiateWriteTraversal> bindModelPaths(Views views) throws IllegalArgumentException {

        Map<String, PathTraversersFactory> allFieldProcessorFactories = Maps.newHashMap();

        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupSteps = new HashMap<>();
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupIdCentricSteps = new HashMap<>();

        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();

            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> accumulate = groupSteps;
            if (idCentric) {
                accumulate = groupIdCentricSteps;
            }

            for (ModelPath modelPath : viewBinding.getModelPaths()) {
                assertNoInstanceIdBindings(viewBinding, modelPath);

                String factoryKey = viewBinding.getViewClassName() + "_" + modelPath.getId();
                if (allFieldProcessorFactories.containsKey(factoryKey)) {
                    throw new IllegalArgumentException("you have already created this binding:" + factoryKey);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("MODELPATH " + modelPath);
                }

                long modelPathHashcode = viewPathKeyProvider.modelPathHashcode(modelPath.getId());
                PathTraversersFactory fieldProcessorFactory = new PathTraversersFactory(viewClassName, modelPathHashcode, modelPath);

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Bind:{}", factoryKey);
                }
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

                List<TraversablePath> pathTraversers = fieldProcessorFactory.buildPathTraversers(viewIdFieldName);
                groupPathTraverserByClass(accumulate, pathTraversers);

                List<TraversablePath> initialBackRefStep = fieldProcessorFactory.buildBackPathTraversers(viewIdFieldName);
                if (initialBackRefStep != null) {
                    groupPathTraverserByClass(accumulate, initialBackRefStep);
                }
            }

        }
        return buildInitialStepDispatchers(groupSteps);
    }

    private void assertNoInstanceIdBindings(ViewBinding viewBinding, ModelPath modelPath) {
        for (ModelPathStep step : modelPath.getPathMembers()) {
            if (step.getStepType() == ModelPathStepType.value) {
                for (String fieldName : step.getFieldNames()) {
                    if (ReservedFields.INSTANCE_ID.equals(fieldName)) {
                        throw (new IllegalStateException("It is illegal to directly bind to the '" + ReservedFields.INSTANCE_ID + "' field.  " + viewBinding));
                    }
                }
            }
        }
    }

    private Map<String, InitiateWriteTraversal> buildInitialStepDispatchers(
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupSteps) {

        Map<String, InitiateWriteTraversal> all = new HashMap<>();
        for (String eventClassName : groupSteps.keySet()) {
            Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>> typedSteps = groupSteps.get(eventClassName);

            ArrayListMultimap<InitiateTraverserKey, TraversablePath> valueTraversers = typedSteps.get(ModelPathStepType.value);

            ArrayListMultimap<InitiateTraverserKey, TraversablePath> refTraversers = ArrayListMultimap.create();
            if (typedSteps.get(ModelPathStepType.ref) != null) {
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> refPaths = typedSteps.get(ModelPathStepType.ref);
                refTraversers.putAll(refPaths);
            }
            if (typedSteps.get(ModelPathStepType.refs) != null) {
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> refsPaths = typedSteps.get(ModelPathStepType.refs);
                refTraversers.putAll(refsPaths);
            }

            ArrayListMultimap<InitiateTraverserKey, TraversablePath> backRefTraversers = ArrayListMultimap.create();
            if (typedSteps.get(ModelPathStepType.backRefs) != null) {
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> backRefsPaths = typedSteps.get(ModelPathStepType.backRefs);
                backRefTraversers.putAll(backRefsPaths);
            }
            if (typedSteps.get(ModelPathStepType.latest_backRef) != null) {
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> latestBackRefsPaths = typedSteps.get(ModelPathStepType.latest_backRef);
                backRefTraversers.putAll(latestBackRefsPaths);
            }
            if (typedSteps.get(ModelPathStepType.count) != null) {
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> countPaths = typedSteps.get(ModelPathStepType.count);
                backRefTraversers.putAll(countPaths);
            }
            ConcurrencyChecker concurrencyChecker = new ConcurrencyChecker(concurrencyStore);

//            InitiateWriteTraversal initiateTraversal = new InitiateWriteTraversal(
//                    concurrencyChecker,
//                    transformToPathAtATime(valueTraversers),
//                    referenceStore,
//                    transformToPathAtATime(refTraversers),
//                    transformToPathAtATime(backRefTraversers));
            InitiateWriteTraversal initiateTraversal = new InitiateWriteTraversal(concurrencyChecker,
                transformToPrefixCollapsedTree("values", valueTraversers),
                referenceStore,
                transformToPrefixCollapsedTree("refs", refTraversers),
                transformToPrefixCollapsedTree("backrefs", backRefTraversers));

            all.put(eventClassName, initiateTraversal);
        }

        return all;
    }

    ListMultimap<InitiateTraverserKey, PathTraverser> transformToPrefixCollapsedTree(String family,
        ListMultimap<InitiateTraverserKey, TraversablePath> traversablePaths) {
        if (traversablePaths == null) {
            return null;
        }
        ListMultimap<InitiateTraverserKey, PathTraverser> transformed = ArrayListMultimap.create();
        for (InitiateTraverserKey key : traversablePaths.keySet()) {
            Map<PathTraverserKey, StepTree> subTrees = new ConcurrentHashMap<>();
            for (TraversablePath traversablePath : traversablePaths.get(key)) {
                InitiateTraversalContext initialStepContext = traversablePath.getInitialStepContext();
                PathTraverserKey pathTraverserKey = new PathTraverserKey(initialStepContext.getInitialFieldNames(),
                    initialStepContext.getPathIndex(),
                    initialStepContext.getMembersSize());

                StepTree stepTree = subTrees.get(pathTraverserKey);
                if (stepTree == null) {
                    stepTree = new StepTree();
                    subTrees.put(pathTraverserKey, stepTree);
                }
                stepTree.add(traversablePath.getStepTraversers());
            }

            for (PathTraverserKey pathTraverserKey : subTrees.keySet()) {
                StepTree stepTree = subTrees.get(pathTraverserKey);
                PathTraverser pathTraverser = new PathTraverser(pathTraverserKey, new PrefixCollapsedStepStreamerFactory(stepTree));
                transformed.put(key, pathTraverser);
            }
        }
        return transformed;
    }

    ListMultimap<InitiateTraverserKey, PathTraverser> transformToPathAtATime(ListMultimap<InitiateTraverserKey, TraversablePath> traversablePaths) {
        if (traversablePaths == null) {
            return null;
        }
        ListMultimap<InitiateTraverserKey, PathTraverser> transformed = ArrayListMultimap.create();
        for (InitiateTraverserKey initiateTraverserKey : traversablePaths.keySet()) {
            for (TraversablePath traversablePath : traversablePaths.get(initiateTraverserKey)) {
                InitiateTraversalContext initialStepContext = traversablePath.getInitialStepContext();
                PathTraverserKey pathTraverserKey = new PathTraverserKey(initialStepContext.getInitialFieldNames(),
                    initialStepContext.getPathIndex(),
                    initialStepContext.getMembersSize());
                PathTraverser pathTraverser = new PathTraverser(pathTraverserKey,
                    new PathAtATimeStepStreamerFactory(traversablePath.getStepTraversers()));
                transformed.put(initiateTraverserKey, pathTraverser);
            }
        }
        return transformed;
    }

    private void groupPathTraverserByClass(
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupedPathTraversers,
        List<TraversablePath> pathTraverers) {

        for (TraversablePath traversablePath : pathTraverers) {
            for (String className : traversablePath.getInitialClassNames()) {
                Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>> typedSteps = groupedPathTraversers.get(className);
                if (typedSteps == null) {
                    typedSteps = Maps.newHashMap();
                    groupedPathTraversers.put(className, typedSteps);
                }
                ModelPathStepType stepType = traversablePath.getInitialModelPathStepType();
                ArrayListMultimap<InitiateTraverserKey, TraversablePath> steps = typedSteps.get(stepType);
                if (steps == null) {
                    steps = ArrayListMultimap.create();
                    typedSteps.put(stepType, steps);
                }

                String refFieldName = traversablePath.getRefFieldName();
                if (refFieldName != null) {
                    steps.put(new InitiateTraverserKey(refFieldName, refFieldName), traversablePath);
                }

                for (String fieldName : traversablePath.getInitialFieldNames()) {
                    steps.put(new InitiateTraverserKey(fieldName, refFieldName), traversablePath);
                }
            }
        }
    }
}

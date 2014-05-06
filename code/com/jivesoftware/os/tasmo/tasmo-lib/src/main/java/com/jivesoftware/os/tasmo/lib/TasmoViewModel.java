package com.jivesoftware.os.tasmo.lib;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.id.ChainedVersion;
import com.jivesoftware.os.tasmo.id.TenantId;
import com.jivesoftware.os.tasmo.lib.concur.ConcurrencyChecker;
import com.jivesoftware.os.tasmo.lib.process.traversal.*;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceStore;
import com.jivesoftware.os.tasmo.reference.lib.concur.ConcurrencyStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TasmoViewModel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantId masterTenantId;
    private final ViewsProvider viewsProvider;
    private final ConcurrentHashMap<TenantId, VersionedTasmoViewModel> versionedViewModels;
    private final ConcurrencyStore concurrencyStore;
    private final ReferenceStore referenceStore;
    private final StripingLocksProvider<TenantId> loadModelLocks = new StripingLocksProvider<>(1024);
    private final ListeningExecutorService pathExecutors;

    public TasmoViewModel(ListeningExecutorService pathExecutors,
            TenantId masterTenantId,
            ViewsProvider viewsProvider,
            ConcurrencyStore concurrencyStore,
            ReferenceStore referenceStore) {
        this.pathExecutors = pathExecutors;
        this.masterTenantId = masterTenantId;
        this.viewsProvider = viewsProvider;
        this.concurrencyStore = concurrencyStore;
        this.referenceStore = referenceStore;
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

    public void loadModel(TenantId tenantId) {
        synchronized (loadModelLocks.lock(tenantId)) {
            ChainedVersion currentVersion = viewsProvider.getCurrentViewsVersion(tenantId);
            if (currentVersion == ChainedVersion.NULL) {
                versionedViewModels.put(tenantId, new VersionedTasmoViewModel(ChainedVersion.NULL, null, null, null));
            } else {
                VersionedTasmoViewModel currentVersionedViewsModel = versionedViewModels.get(tenantId);
                if (currentVersionedViewsModel == null
                        || !currentVersionedViewsModel.getVersion().equals(currentVersion)) {

                    Views views = viewsProvider.getViews(new ViewsProcessorId(tenantId, "NotBeingUsedYet"));

                    if (views != null) {
                        ListMultimap<String, InitiateTraversal> dispatchers = bindModelPaths(views);
                        SetMultimap<String, FieldNameAndType> eventModel = bindEventFieldTypes(views);
                        Set<String> notifiableViewClassNames = buildNotifiableViewClassNames(views);
                        versionedViewModels.put(tenantId, new VersionedTasmoViewModel(views.getVersion(), dispatchers, eventModel, notifiableViewClassNames));
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldNameAndType that = (FieldNameAndType) o;

            if (idCentric != that.idCentric) return false;
            if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) return false;
            if (fieldType != that.fieldType) return false;

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

    private ListMultimap<String, InitiateTraversal> bindModelPaths(Views views) throws IllegalArgumentException {

        Map<String, PathTraversersFactory> allFieldProcessorFactories = Maps.newHashMap();

        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupSteps = new HashMap<>();
        Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupIdCentricSteps = new HashMap<>();

        for (ViewBinding viewBinding : views.getViewBindings()) {

            String viewClassName = viewBinding.getViewClassName();
            String viewIdFieldName = viewBinding.getViewIdFieldName();
            boolean idCentric = viewBinding.isIdCentric();

            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> accumlulate = groupSteps;
            if (idCentric) {
                accumlulate = groupIdCentricSteps;
            }

            for (ModelPath modelPath : viewBinding.getModelPaths()) {

                String factoryKey = viewBinding.getViewClassName() + "_" + modelPath.getId();
                if (allFieldProcessorFactories.containsKey(factoryKey)) {
                    throw new IllegalArgumentException("you have already created this binding:" + factoryKey);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("MODELPATH " + modelPath);
                }
                PathTraversersFactory fieldProcessorFactory = new PathTraversersFactory(viewClassName, modelPath, referenceStore);

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Bind:{}", factoryKey);
                }
                allFieldProcessorFactories.put(factoryKey, fieldProcessorFactory);

                List<TraversablePath> pathTraversers = fieldProcessorFactory.buildPathTraversers(viewIdFieldName);
                groupPathTraverserByClass(accumlulate, pathTraversers);

                List<TraversablePath> initialBackRefStep = fieldProcessorFactory.buildBackPathTraversers(viewIdFieldName);
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
            Map<String, Map<ModelPathStepType, ArrayListMultimap<InitiateTraverserKey, TraversablePath>>> groupSteps) {

        ListMultimap<String, InitiateTraversal> all = ArrayListMultimap.create();
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

//            InitiateTraversal initiateTraversal = new InitiateTraversal(
//                    pathExecutors,
//                    concurrencyChecker,
//                    transformToPathAtATime(valueTraversers),
//                    referenceStore,
//                    transformToPathAtATime(refTraversers),
//                    transformToPathAtATime(backRefTraversers));

            InitiateTraversal initiateTraversal = new InitiateTraversal(pathExecutors,
                    concurrencyChecker,
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
            LOG.trace("-------- " + family + " TRIGGER:" + key);
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

                LOG.trace("???????????? " + traversablePath);
                LOG.trace("---------------- Steps:" + Joiner.on(" || ").join(traversablePath.getStepTraversers()));

                stepTree.add(traversablePath.getStepTraversers());
            }

            for (PathTraverserKey pathTraverserKey : subTrees.keySet()) {
                StepTree stepTree = subTrees.get(pathTraverserKey);
//                System.out.println("*** Prefix Tree ***");
//                System.out.println(pathTraverserKey);
//                stepTree.print();
//                System.out.println("******");
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

//for (String eventClassName : all.keySet()) {
//                LOG.trace("---- Traversers for class:" + eventClassName);
//                for (InitiateTraversal initiateTraversal : all.get(eventClassName)) {
//
//                    ArrayListMultimap<InitiateTraverserKey, PathTraverser> valueTraversers = initiateTraversal.getValueTraversers();
//                    if (valueTraversers != null) {
//                        for (InitiateTraverserKey key : valueTraversers.keySet()) {
//                            LOG.trace("-------- Value TRIGGER:" + key);
//                            List<PathTraverser> pathTraversers = valueTraversers.get(key);
//                            TraversalTree prefixMergingTree = new TraversalTree();
//                            for (PathTraverser pathTraverser : pathTraversers) {
//                                LOG.trace("???????????? "+pathTraverser);
//                                LOG.trace("---------------- Steps:" + Joiner.on(" || ").join(pathTraverser.getStepTraversers()));
//                                prefixMergingTree.add(pathTraverser.getPathTraverserKey(), pathTraverser.getStepTraversers());
//                            }
//                            LOG.trace("*** Prefix Tree ***");
//                            prefixMergingTree.print();
//                            LOG.trace("******");
//
//                        }
//                    }
//
//                    ArrayListMultimap<InitiateTraverserKey, PathTraverser> backRefTraversers = initiateTraversal.getBackRefTraversers();
//                    if (backRefTraversers != null) {
//                        for (InitiateTraverserKey key : backRefTraversers.keySet()) {
//                            LOG.trace("-------- BackRef TRIGGER:" + key);
//
//                            TraversalTree prefixMergingTree = new TraversalTree();
//                            for (PathTraverser pathTraverser : backRefTraversers.get(key)) {
//                                LOG.trace("???????????? "+pathTraverser);
//                                LOG.trace("---------------- Steps:" + Joiner.on(" || ").join(pathTraverser.getStepTraversers()));
//                                prefixMergingTree.add(pathTraverser.getPathTraverserKey(), pathTraverser.getStepTraversers());
//                            }
//                            LOG.trace("*** Prefix Tree ***");
//                            prefixMergingTree.print();
//                            LOG.trace("******");
//}
//                    }
//
//                    ArrayListMultimap<InitiateTraverserKey, PathTraverser> forwardRefTraversers = initiateTraversal.getForwardRefTraversers();
//                    if (forwardRefTraversers != null) {
//                        for (InitiateTraverserKey key : forwardRefTraversers.keySet()) {
//                            LOG.trace("-------- Ref TRIGGER:" + key);
//                            TraversalTree prefixMergingTree = new TraversalTree();
//                            for (PathTraverser pathTraverser : forwardRefTraversers.get(key)) {
//                                LOG.trace("???????????? "+pathTraverser);
//                                LOG.trace("---------------- Steps:" + Joiner.on(" || ").join(pathTraverser.getStepTraversers()));
//                                prefixMergingTree.add(pathTraverser.getPathTraverserKey(), pathTraverser.getStepTraversers());
//                            }
//                            LOG.trace("*** Prefix Tree ***");
//                            prefixMergingTree.print();
//                            LOG.trace("******");
//}
//                    }
//                    LOG.trace("");
//
//            }
//        }

package com.jivesoftware.os.tasmo.lib.read;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.tasmo.configuration.views.PathAndDictionary;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.lib.write.PathId;
import com.jivesoftware.os.tasmo.lib.write.ViewField;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.reference.lib.ReferenceWithTimestamp;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.service.ViewValueReader;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore.ViewCollector;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**

 @author jonathan.colt
 */
public class ReadCachedViewFields {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ViewValueReader viewValueReader;
    private final TenantViewsProvider tenantViewsProvider;
    private final long viewMaxSizeInBytes; // This is the number of bytes that can be read for a single view. Defends against posion view that could cause OOM

    public ReadCachedViewFields(ViewValueReader viewValueReader,
        TenantViewsProvider viewModel,
        long viewMaxSizeInBytes) {
        this.viewValueReader = viewValueReader;
        this.tenantViewsProvider = viewModel;
        this.viewMaxSizeInBytes = viewMaxSizeInBytes;
    }

    public Map<ViewDescriptor, ViewFieldsResponse> readViews(List<ViewDescriptor> request) throws IOException {
        Preconditions.checkArgument(request != null);

        List<ViewCollectorImpl> viewCollectors = Lists.newArrayList();
        for (ViewDescriptor viewDescriptor : request) {
            Map<Long, PathAndDictionary> viewFieldBindings = tenantViewsProvider.getViewFieldBinding(
                viewDescriptor.getTenantIdAndCentricId().getTenantId(), viewDescriptor.getViewId().getClassName());

            // TODO: be able to pass back error result per descriptor to front end
            if (viewFieldBindings == null) {
                LOG.error(viewDescriptor.getViewId().getClassName() + " has no declared view bindings.");
            } else {
                viewCollectors.add(new ViewCollectorImpl(viewDescriptor, viewFieldBindings, viewMaxSizeInBytes));
            }
        }
        try {
            viewValueReader.readViewValues(viewCollectors);

            Map<ViewDescriptor, ViewFieldsResponse> views = new HashMap<>();
            for (ViewCollectorImpl viewCollector : viewCollectors) {
                views.put(viewCollector.getViewDescriptor(), new ViewFieldsResponse(viewCollector.getViewValueFields()));
            }
            return Collections.unmodifiableMap(views);
        } catch (Exception ex) {
            LOG.error("Failed while loading {}", request);
            throw new IOException("Failed to load for the following reason.", ex);
        }
    }

    static class ViewCollectorImpl implements ViewCollector {

        private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
        private final ViewDescriptor viewDescriptor;
        private final Map<Long, PathAndDictionary> viewClassFieldBindings;
        private final List<ViewField> viewValueFields = new ArrayList<>();
        private final long viewMaxSizeInBytes;
        private long viewSizeInBytes;

        ViewCollectorImpl(
            ViewDescriptor viewDescriptor,
            Map<Long, PathAndDictionary> viewClassFieldBindings,
            long viewMaxSizeInBytes) {
            this.viewDescriptor = viewDescriptor;
            this.viewClassFieldBindings = viewClassFieldBindings;
            this.viewMaxSizeInBytes = viewMaxSizeInBytes;
        }

        @Override
        public ViewDescriptor getViewDescriptor() {
            return viewDescriptor;
        }

        public List<ViewField> getViewValueFields() {
            return viewValueFields;
        }

        @Override // TODO HUGE Method please refactor
        public ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> callback(
            ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> fieldValue) throws Exception {
            if (viewClassFieldBindings == null) { // if factored out so that we don't exceed 4 levels of if nesting.
                return fieldValue;
            }
            if (fieldValue != null) {
                ByteBuffer bb = ByteBuffer.wrap(fieldValue.getColumn().getImmutableBytes());
                long modelPathIdHashCode = bb.getLong();
                PathAndDictionary pathAndDictionary = viewClassFieldBindings.get(modelPathIdHashCode);

                if (pathAndDictionary != null) {
                    ModelPath modelPath = pathAndDictionary.getPath();
                    if (modelPath != null) {
                        long pathComboKey = bb.getLong();
                        String[] viewPathClasses = pathAndDictionary.getDictionary().lookupModelPathClasses(pathComboKey);

                        if (viewPathClasses != null && viewPathClasses.length > 0) {
                            Id[] modelPathIds = modelPathIds(bb, modelPath.getPathMemberSize());
                            ViewValue value = fieldValue.getValue();
                            long[] modelPathTimeStamps = value.getModelPathTimeStamps();
                            List<ReferenceWithTimestamp> referenceWithTimestamps = new ArrayList<>();
                            PathId[] modelPathInstanceIds = new PathId[modelPathTimeStamps.length];
                            for (int i = 0; i < modelPathTimeStamps.length; i++) {
                                ModelPathStep modelPathStep = modelPath.getPathMembers().get(i);
                                ObjectId objectId = new ObjectId(viewPathClasses[i], modelPathIds[i]);
                                modelPathInstanceIds[i] = new PathId(objectId, modelPathTimeStamps[i]);
                                referenceWithTimestamps.add(new ReferenceWithTimestamp(objectId, modelPathStep.getRefFieldName(), modelPathTimeStamps[i]));
                            }

                            viewValueFields.add(new ViewField(-1,
                                viewDescriptor.getActorId(),
                                ViewField.ViewFieldChangeType.add,
                                viewDescriptor.getViewId(),
                                modelPath,
                                modelPathIdHashCode,
                                modelPathInstanceIds,
                                referenceWithTimestamps,
                                modelPathTimeStamps,
                                value.getValue(),
                                fieldValue.getTimestamp()));

                            byte[] rawValue = value.getValue();
                            viewSizeInBytes += (rawValue == null) ? 0 : rawValue.length;
                            if (viewSizeInBytes > viewMaxSizeInBytes) {
                                LOG.error("ViewDescriptor:" + viewDescriptor + " is larger than viewMaxReadableBytes:" + viewMaxSizeInBytes);
                                return null;
                            }
                        } else {
                            LOG.
                                warn("Unable to look up model path " + modelPathIdHashCode
                                    + " classes for view path with path combination key: " + pathComboKey
                                    + " dropping value: " + fieldValue.getValue() + " on the floor.");
                        }
                    } else {
                        LOG.warn("failed to load ViewValueBinding for viewValueBindingKey={}, fieldValue={} ", new Object[]{ modelPathIdHashCode,
                            fieldValue });
                    }
                } else {
                    LOG.debug("Failed to load model path and view path dictionary "
                        + "from column key. Older column key format is likely the case. viewDescriptor:" + viewDescriptor + " fieldValue:" + fieldValue);
                }
            }
            return fieldValue;
        }

        private Id[] modelPathIds(ByteBuffer bb, int count) {
            Id[] ids = new Id[count];
            for (int i = 0; i < count; i++) {
                int l = bb.get();
                byte[] idBytes = new byte[l];
                bb.get(idBytes);
                ids[i] = new Id(idBytes);
            }
            return ids;
        }

    }
}

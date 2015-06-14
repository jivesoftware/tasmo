package com.jivesoftware.os.tasmo.view.reader.service.shared;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.CallbackStream;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.MultiRemove;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnTimestampRemove;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantKeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.tasmo.id.ViewValue;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewWriteFieldChange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pete
 */
public class ViewValueStore {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore;
    private final ViewPathKeyProvider viewPathKeyProvider;

    public ViewValueStore(RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> viewValueStore,
        ViewPathKeyProvider viewPathKeyProvider) {
        this.viewValueStore = viewValueStore;
        this.viewPathKeyProvider = viewPathKeyProvider;
    }

    public ImmutableByteArray rowKey(ObjectId viewObjectId) throws IOException {
        ByteArrayOutputStream key = new ByteArrayOutputStream();
        key.write(viewObjectId.toLexBytes().array());
        return new ImmutableByteArray(key.toByteArray());
    }

    private ImmutableByteArray columnKey(long modelPathId, ObjectId[] modelPathInstanceIds) throws IOException {
        ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
        keyStream.write(longBytes(modelPathId));
        keyStream.write(longBytes(viewPathKey(modelPathInstanceIds)));
        if (modelPathInstanceIds != null) {
            Id[] allRawPathIds = allRawPathIds(modelPathInstanceIds);
            for (Id id : allRawPathIds) {
                keyStream.write(id.toLengthPrefixedBytes());
            }
        }
        return new ImmutableByteArray(keyStream.toByteArray());
    }

    private byte[] longBytes(long v) {
        return longBytes(v, new byte[8], 0);
    }

    private byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    private long viewPathKey(ObjectId[] objectIds) {
        String[] classes = new String[objectIds.length];
        for (int i = 0; i < objectIds.length; i++) {
            classes[i] = objectIds[i].getClassName();
        }
        return viewPathKeyProvider.pathKeyHashcode(classes);
    }

    private Id[] allRawPathIds(ObjectId[] pathId) {
        Id[] orderIds = new Id[pathId.length];
        for (int i = 0; i < pathId.length; i++) {
            orderIds[i] = pathId[i].getId();
        }
        return orderIds;
    }

    public ViewValue get(TenantIdAndCentricId tenantIdAndCentricId,
        ObjectId viewObjectId, long modelPathId, ObjectId[] modelPathInstanceState) throws IOException {
        ImmutableByteArray rowKey = rowKey(viewObjectId);
        ImmutableByteArray columnKey = columnKey(modelPathId, modelPathInstanceState);
        ViewValue got = viewValueStore.get(tenantIdAndCentricId, rowKey, columnKey, null, null);
        return got;
    }

    // TODO need a batch version and this has the potential OOM
    public void clear(TenantIdAndCentricId tenantIdAndCentricId, ObjectId viewId, final long timestamp) throws IOException {

        final List<RowColumnTimestampRemove<ImmutableByteArray, ImmutableByteArray>> removes = new ArrayList<>();
        final ConstantTimestamper constantTimestamper = new ConstantTimestamper(timestamp - 1);
        final ImmutableByteArray rowKey = rowKey(viewId);
        viewValueStore.getEntrys(tenantIdAndCentricId, rowKey(viewId), null, Long.MAX_VALUE, 1000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long>>() {

                @Override
                public ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> callback(ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long> v)
                throws
                Exception {
                    if (v != null && v.getTimestamp() < timestamp) {
                        removes.add(new RowColumnTimestampRemove<>(rowKey, v.getColumn(), constantTimestamper));
                    }
                    return v;
                }
            });
        viewValueStore.multiRowsMultiRemove(tenantIdAndCentricId, removes);
    }

    static public interface ViewCollector extends CallbackStream<ColumnValueAndTimestamp<ImmutableByteArray, ViewValue, Long>> {

        ViewDescriptor getViewDescriptor();
    }

    public void multiGet(List<? extends ViewCollector> viewCollectors) throws IOException {
        List<TenantKeyedColumnValueCallbackStream<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, Long>> keyCallbackPairs = Lists.
            newArrayList();
        for (ViewCollector viewCollector : viewCollectors) {
            ViewDescriptor viewDescriptor = viewCollector.getViewDescriptor();
            keyCallbackPairs.add(new TenantKeyedColumnValueCallbackStream<>(viewDescriptor.getTenantIdAndCentricId(),
                rowKey(viewDescriptor.getViewId()), viewCollector));
        }

        viewValueStore.multiRowGetAll(keyCallbackPairs);
    }

    public void add(TenantIdAndCentricId tenantIdAndCentricId, List<ViewWriteFieldChange> adds) throws IOException {
        MultiAdd<ImmutableByteArray, ImmutableByteArray, ViewValue> rawAdds = new MultiAdd<>();
        for (ViewWriteFieldChange change : adds) {
            rawAdds.add(rowKey(change.getViewObjectId()),
                columnKey(change.getModelPathIdHashcode(),
                    change.getModelPathInstanceIds()),
                change.getValue(),
                new ConstantTimestamper(change.getTimestamp()));

            LOG.debug("VVS:ADD {}", change);
        }
        List<RowColumValueTimestampAdd<ImmutableByteArray, ImmutableByteArray, ViewValue>> took = rawAdds.take();
        viewValueStore.multiRowsMultiAdd(tenantIdAndCentricId, took);
    }

    public void remove(TenantIdAndCentricId tenantIdAndCentricId, List<ViewWriteFieldChange> removes) throws IOException {
        MultiRemove<ImmutableByteArray, ImmutableByteArray> rawRemoves = new MultiRemove<>();
        for (ViewWriteFieldChange change : removes) {
            rawRemoves.add(rowKey(change.getViewObjectId()),
                columnKey(change.getModelPathIdHashcode(),
                    change.getModelPathInstanceIds()),
                new ConstantTimestamper(change.getTimestamp()));

            LOG.debug("VVS:REMOVED {}", change);
        }
        List<RowColumnTimestampRemove<ImmutableByteArray, ImmutableByteArray>> took = rawRemoves.take();
        viewValueStore.multiRowsMultiRemove(tenantIdAndCentricId, took);
    }

    public void remove(TenantIdAndCentricId tenantIdAndCentricId, ImmutableByteArray rowKey, ImmutableByteArray columnKey, long when) throws IOException {
        viewValueStore.remove(tenantIdAndCentricId, rowKey, columnKey, new ConstantTimestamper(when));
    }
}

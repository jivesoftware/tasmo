package com.jivesoftware.os.tasmo.view.reader.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ImmutableByteArray;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.tasmo.configuration.views.TenantViewsProvider;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.path.StringHashcodeViewPathKeyProvider;
import com.jivesoftware.os.tasmo.model.path.ViewPathKeyProvider;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValue;
import com.jivesoftware.os.tasmo.view.reader.service.shared.ViewValueStore;
import com.jivesoftware.os.tasmo.view.reader.service.writer.ViewValueWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ViewProviderTest {

    ViewPermissionChecker viewPermissionChecker;
    ViewValueReader viewValueReader;
    TenantViewsProvider tenantViewsProvider;
    ViewProvider viewProvider;
    ViewFormatter viewFormatter;
    JsonViewMerger merger = new JsonViewMerger(new ObjectMapper());
    StaleViewFieldStream staleViewFieldStream;
    RowColumnValueStore<TenantIdAndCentricId, ImmutableByteArray, ImmutableByteArray, ViewValue, RuntimeException> store;

    @BeforeMethod
    public void setUpMethod() throws Exception {

        ViewsProvider viewsProvider = new ViewsProvider() {

            @Override
            public ChainedVersion getCurrentViewsVersion(TenantId tenantId) {
                return new ChainedVersion("", "1");
            }

            @Override
            public Views getViews(ViewsProcessorId viewsProcessorId) {
                List<ViewBinding> viewBindings = new ArrayList<>();
                List<ModelPath> modelPaths = new ArrayList<>();
                ModelPathStep modelPathStep = new ModelPathStep(true, ImmutableSet.of("view"), null, ModelPathStepType.value, null, Arrays.asList("title"));
                ModelPath modelPath = ModelPath.builder("1").addPathMember(modelPathStep).build();
                modelPaths.add(modelPath);
                viewBindings.add(new ViewBinding("view", modelPaths, true, false, false, "id"));
                return new Views(viewsProcessorId.getTenantId(), getCurrentViewsVersion(viewsProcessorId.getTenantId()), viewBindings);
            }
        };

        tenantViewsProvider = new TenantViewsProvider(new TenantId("master"), viewsProvider, new StringHashcodeViewPathKeyProvider());
        staleViewFieldStream = Mockito.mock(StaleViewFieldStream.class);
        viewPermissionChecker = new ViewPermissionChecker() {

            @Override
            public ViewPermissionCheckResult check(TenantId tenantId, Id actorId, final Set<Id> permissionCheckTheseIds) {
                return new ViewPermissionCheckResult() {

                    @Override
                    public Set<Id> allowed() {
                        return permissionCheckTheseIds;
                    }

                    @Override
                    public Set<Id> denied() {
                        return new HashSet<>();
                    }

                    @Override
                    public Set<Id> unknown() {
                        return new HashSet<>();
                    }
                };
            }
        };
        viewFormatter = new ViewAsObjectNode();
        store = new RowColumnValueStoreImpl<>();
        viewValueReader = new ViewValueReader(new ViewValueStore(store, new StringHashcodeViewPathKeyProvider()));

        viewProvider = new ViewProvider(viewPermissionChecker,
                viewValueReader,
                tenantViewsProvider,
                viewFormatter,
                merger,
                staleViewFieldStream,
                1_024L * 1_024L * 10);

    }

    @Test
    public void testReadView() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TenantId tenantId = new TenantId("tenantId");
        ViewPathKeyProvider viewPathKeyProvider = new StringHashcodeViewPathKeyProvider();
        ViewValueWriter viewValueWriter = new ViewValueWriter(new ViewValueStore(store, viewPathKeyProvider));
        ViewValueWriter.Transaction transaction = viewValueWriter.begin(new TenantIdAndCentricId(tenantId, new Id(1)));
        transaction.set(new ObjectId("view", new Id(2)), viewPathKeyProvider.modelPathHashcode("1"), new ObjectId[]{new ObjectId("view", new Id(2))},
                new ViewValue(new long[]{1}, mapper.writeValueAsBytes(ImmutableMap.of("title", "booya"))), 1);
        viewValueWriter.commit(transaction);

        ViewDescriptor request = new ViewDescriptor(new TenantIdAndCentricId(tenantId, new Id(1)), new Id(1), new ObjectId("view", new Id(2)));
        ViewResponse readView = (ViewResponse) viewProvider.readView(request);
        System.out.println("Result:" + readView + " " + readView.getClass());
        ObjectNode viewBody = readView.getViewBody();
        Assert.assertNotNull(viewBody);
        Assert.assertEquals(viewBody.get("title").asText(), "booya");
    }

    @Test
    public void testPoisonView() throws Exception {

        viewProvider = new ViewProvider(viewPermissionChecker,
                viewValueReader,
                tenantViewsProvider,
                viewFormatter,
                merger,
                staleViewFieldStream,
                1); // any view over 1 byte should fail.


        ObjectMapper mapper = new ObjectMapper();
        TenantId tenantId = new TenantId("tenantId");
        ViewPathKeyProvider viewPathKeyProvider = new StringHashcodeViewPathKeyProvider();
        ViewValueWriter viewValueWriter = new ViewValueWriter(new ViewValueStore(store, viewPathKeyProvider));
        ViewValueWriter.Transaction transaction = viewValueWriter.begin(new TenantIdAndCentricId(tenantId, new Id(1)));
        transaction.set(new ObjectId("view", new Id(2)), viewPathKeyProvider.modelPathHashcode("1"), new ObjectId[]{new ObjectId("view", new Id(2))},
                new ViewValue(new long[]{1}, mapper.writeValueAsBytes(ImmutableMap.of("title", "booya"))), 1);
        viewValueWriter.commit(transaction);

        ViewDescriptor request = new ViewDescriptor(new TenantIdAndCentricId(tenantId, new Id(1)), new Id(1), new ObjectId("view", new Id(2)));
        ViewResponse readView = (ViewResponse) viewProvider.readView(request);
        System.out.println("Result:" + readView + " " + readView.getClass());

    }
}

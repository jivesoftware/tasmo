/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.configuration.views;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.id.ChainedVersion;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.model.Views;
import com.jivesoftware.os.tasmo.model.ViewsProcessorId;
import com.jivesoftware.os.tasmo.model.ViewsProvider;
import com.jivesoftware.os.tasmo.model.path.ModelPath;
import com.jivesoftware.os.tasmo.model.path.ModelPathStep;
import com.jivesoftware.os.tasmo.model.path.ModelPathStepType;
import com.jivesoftware.os.tasmo.model.path.StringHashcodeViewPathKeyProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class ViewModelTest {

    private final TenantId tenantId = new TenantId("master");

    private ViewsProvider viewsProvider;
    private TenantViewsProvider viewModel;

    @BeforeMethod
    public void setUpMethod() throws Exception {

        viewsProvider = Mockito.mock(ViewsProvider.class);
        viewModel = new TenantViewsProvider(tenantId, viewsProvider, new StringHashcodeViewPathKeyProvider());
    }

    /**
     * Test of loadModel method, of class ViewModel.
     */
    @Test
    public void testLoadModel() throws Exception {
        System.out.println("loadModel");

        ChainedVersion version1 = new ChainedVersion("0", "1");
        ChainedVersion version2 = new ChainedVersion("1", "2");
        Views views1 = makeViews("Foo", version1, "A", "x", "y", "z");
        Views views2 = makeViews("Bar", version2, "B", "j", "k", "l");

        Mockito.when(viewsProvider.getCurrentViewsVersion(tenantId)).thenReturn(version1, version2);
        Mockito.when(viewsProvider.getViews(Mockito.any(ViewsProcessorId.class))).thenReturn(views1, views2);

        viewModel.loadModel(tenantId);
        Map<Long, PathAndDictionary> viewFieldBinding = viewModel.getViewFieldBinding(tenantId, "Bar");
        Assert.assertNull(viewFieldBinding);
        viewFieldBinding = viewModel.getViewFieldBinding(tenantId, "Foo");
        Assert.assertNotNull(viewFieldBinding);
        Assert.assertTrue(viewFieldBinding.size() == 1);
        for (PathAndDictionary path : viewFieldBinding.values()) {
            Assert.assertEquals(Sets.newHashSet("A"), path.getPath().getRootClassNames());
        }

        viewModel.loadModel(tenantId);
        viewFieldBinding = viewModel.getViewFieldBinding(tenantId, "Foo");
        Assert.assertNull(viewFieldBinding);
        viewFieldBinding = viewModel.getViewFieldBinding(tenantId, "Bar");
        Assert.assertNotNull(viewFieldBinding);
        Assert.assertTrue(viewFieldBinding.size() == 1);
        for (PathAndDictionary path : viewFieldBinding.values()) {
            Assert.assertEquals(Sets.newHashSet("B"), path.getPath().getRootClassNames());
        }

    }

    private Views makeViews(String className, ChainedVersion version, String pathName, String... fieldNames) {
        List<ModelPath> modelPaths = new ArrayList<>();
        Set<String> sourceClasses = new HashSet<>();
        sourceClasses.add(pathName);
        modelPaths.add(ModelPath.builder(pathName)
            .addPathMember(new ModelPathStep(true, sourceClasses, null, ModelPathStepType.value, null, Arrays.asList(fieldNames), false))
            .build());
        ViewBinding viewBinding = new ViewBinding(className, modelPaths, false, false, false, null);
        List<ViewBinding> viewBindings = new ArrayList<>();
        viewBindings.add(viewBinding);
        return new Views(tenantId, version, viewBindings);
    }
}

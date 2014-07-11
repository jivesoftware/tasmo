package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

/**
 *
 * @author jonathan.colt
 */
public class BaseTest {

    TenantId tenantId;
    TenantIdAndCentricId tenantIdAndCentricId;
    Id actorId;
    ObjectMapper mapper = new ObjectMapper();

    {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    }

    @BeforeMethod
    public void setup() throws Exception {

        tenantId = new TenantId("test");
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        actorId = new Id(1L);
    }

    @DataProvider (name = "tasmoMaterializer")
    public Iterator<Object[]> tasmoMaterializer() throws Exception {

        List<Object[]> paramList = new ArrayList<>();
        paramList.add(new Object[]{ asyncHarness() });
        paramList.add(new Object[]{ syncHarness() });
        //paramList.add(new Object[]{ syncWithAsyncReadMaterializerHarness() });
        return paramList.iterator();
    }

    @DataProvider (name = "tasmoAsyncOnluMaterializer")
    public Iterator<Object[]> tasmoAsyncMaterializer() throws Exception {

        List<Object[]> paramList = new ArrayList<>();
        paramList.add(new Object[]{ asyncHarness(), syncHarness() });

        return paramList.iterator();
    }



    private TasmoMaterializerHarness syncWithAsyncReadMaterializerHarness() throws Exception {
        return TasmoMaterializerHarnessFactory.createSynWriteNotificationReadMaterializer(
            TasmoMaterializerHarnessFactory.createOrderIdProvider(),
            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
            TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
            TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }

    private TasmoMaterializerHarness asyncHarness() throws Exception {
        return TasmoMaterializerHarnessFactory.createWriteTimeMaterializer(
            TasmoMaterializerHarnessFactory.createOrderIdProvider(),
            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
            TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
            TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
            TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }

    private TasmoMaterializerHarness syncHarness() throws Exception {
        return TasmoMaterializerHarnessFactory.createSyncWriteSyncReadsMaterializer(
            TasmoMaterializerHarnessFactory.createOrderIdProvider(),
            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
            TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
            TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
            TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }
}

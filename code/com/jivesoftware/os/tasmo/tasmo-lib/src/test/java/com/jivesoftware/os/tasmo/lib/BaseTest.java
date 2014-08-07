package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdAndCentricId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
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

        tenantId = new TenantId(UUID.randomUUID().toString());
        tenantIdAndCentricId = new TenantIdAndCentricId(tenantId, Id.NULL);
        actorId = Id.NULL;
    }

    @DataProvider(name = "tasmoMaterializer")
    public Iterator<Object[]> tasmoMaterializer() throws Exception {

        List<Object[]> paramList = new ArrayList<>();
        //paramList.add(new Object[]{asyncHarness(TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider())});
        paramList.add(new Object[]{syncHarness(TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider())});
//        paramList.add(new Object[]{syncWithAsyncReadMaterializerHarness(TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
//            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider())});
//
//        if (2 + 2 == 5) {
//            try {
//                TasmoStorageProvider async = TasmoMaterializerHarnessFactory.createEmbeddedHBaseBackStorageProvider("async");
//                TasmoStorageProvider sync = TasmoMaterializerHarnessFactory.createEmbeddedHBaseBackStorageProvider("sync");
//
//                paramList.add(new Object[]{asyncHarness(async)});
//                paramList.add(new Object[]{syncHarness(sync)});
//                paramList.add(new Object[]{syncWithAsyncReadMaterializerHarness(async, sync)});
//            } catch (Exception x) {
//                x.printStackTrace();
//            }
//        }
        return paramList.iterator();
    }

    @DataProvider(name = "tasmoAsyncOnluMaterializer")
    public Iterator<Object[]> tasmoAsyncMaterializer() throws Exception {

        List<Object[]> paramList = new ArrayList<>();
        paramList.add(new Object[]{asyncHarness(TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider()),
            syncHarness(TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider())});

        return paramList.iterator();
    }

    private TasmoMaterializerHarness syncWithAsyncReadMaterializerHarness(
            TasmoStorageProvider asyncTasmoStorageProvider,
            TasmoStorageProvider syncTasmoStorageProvider) throws Exception {
        return TasmoMaterializerHarnessFactory.createSynWriteNotificationReadMaterializer(
                TasmoMaterializerHarnessFactory.createOrderIdProvider(),
                asyncTasmoStorageProvider,
                syncTasmoStorageProvider,
                TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
                TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }

    private TasmoMaterializerHarness asyncHarness(
            TasmoStorageProvider asyncTasmoStorageProvider) throws Exception {
        return TasmoMaterializerHarnessFactory.createWriteTimeMaterializer(
                TasmoMaterializerHarnessFactory.createOrderIdProvider(),
                asyncTasmoStorageProvider,
                TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
                TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
                TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }

    private TasmoMaterializerHarness syncHarness(TasmoStorageProvider syncTasmoStorageProvider) throws Exception {
        return TasmoMaterializerHarnessFactory.createSyncWriteSyncReadsMaterializer(
                TasmoMaterializerHarnessFactory.createOrderIdProvider(),
                syncTasmoStorageProvider,
                TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
                TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
                TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker());
    }
}

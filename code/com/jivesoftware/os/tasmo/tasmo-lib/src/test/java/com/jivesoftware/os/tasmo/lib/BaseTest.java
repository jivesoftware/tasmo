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

    @DataProvider(name = "tasmoMaterializer")
    public Iterator<Object[]> tasmoMaterializer() throws Exception {

        List<Object[]> paramList = new ArrayList<>();
//        paramList.add(new Object[]{
//                TasmoMaterializerHarnessFactory.createWriteTimeMaterializer(
//            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
//            TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
//            TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
//            TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker())});

        paramList.add(new Object[]{TasmoMaterializerHarnessFactory.createSyncWriteSyncReadsMaterializer(
            TasmoMaterializerHarnessFactory.createInmemoryTasmoStorageProvider(),
            TasmoMaterializerHarnessFactory.createNoOpEventBookkeeper(),
            TasmoMaterializerHarnessFactory.createNoOpViewChangeNotificationProcessor(),
            TasmoMaterializerHarnessFactory.createNoOpViewPermissionChecker())});

        return paramList.iterator();
    }
}

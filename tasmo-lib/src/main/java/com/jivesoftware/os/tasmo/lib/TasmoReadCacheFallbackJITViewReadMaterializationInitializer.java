package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.tasmo.lib.model.TasmoViewModel;
import com.jivesoftware.os.tasmo.lib.modifier.ModifierStore;
import com.jivesoftware.os.tasmo.lib.read.ReadCacheFallbackJITReadMaterializeViewProvider;
import com.jivesoftware.os.tasmo.lib.read.ReadCachedViewFields;
import com.jivesoftware.os.tasmo.lib.read.ReadMaterializerViewFields;
import com.jivesoftware.os.tasmo.lib.write.CommitChange;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReadMaterializer;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import com.jivesoftware.os.tasmo.view.reader.service.JsonViewMerger;
import com.jivesoftware.os.tasmo.view.reader.service.ViewAsObjectNode;
import com.jivesoftware.os.tasmo.view.reader.service.ViewFormatter;
import com.jivesoftware.os.tasmo.view.reader.service.ViewPermissionChecker;
import org.merlin.config.Config;

/**
 *
 *
 */
public class TasmoReadCacheFallbackJITViewReadMaterializationInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static interface TasmoReadCacheFallbackJITViewReadMaterializationConfig extends Config {
    }

    public static TasmoServiceHandle<ViewReadMaterializer<ViewResponse>> initialize(TasmoReadCacheFallbackJITViewReadMaterializationConfig config,
        TasmoViewModel tasmoViewModel,
        ReadMaterializerViewFields readMaterializerViewFields,
        ReadCachedViewFields readCachedViewFields,
        ViewPermissionChecker viewPermissionChecker,
        Optional<ModifierStore> modifierStore,
        Optional<CommitChange> commitChangeVistor) throws Exception {

        ViewFormatter<ViewResponse> viewFormatter = new ViewAsObjectNode();

        final ViewReadMaterializer<ViewResponse> viewReadMaterializer = new ReadCacheFallbackJITReadMaterializeViewProvider<>(readCachedViewFields,
            readMaterializerViewFields,
            modifierStore,
            viewPermissionChecker,
            viewFormatter,
            new JsonViewMerger(new ObjectMapper()));

        return new TasmoServiceHandle<ViewReadMaterializer<ViewResponse>>() {

            @Override
            public ViewReadMaterializer<ViewResponse> getService() {
                return viewReadMaterializer;
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void stop() throws Exception {
            }
        };
    }

}

package org.apache.pinot.integration.tests;

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;

/**
 * Integration test that extends LLCRealtimeClusterIntegrationTest but with split commit enabled and segment commit end
 * with metadata file upload enabled.
 */
public class LLCRealtimeClusterSplitCommitEndWithMetadataTest extends LLCRealtimeClusterIntegrationTest {

    @Override
    public void startController() {
        ControllerConf controllerConfig = getDefaultControllerConfiguration();
        controllerConfig.setSplitCommit(true);
        startController(controllerConfig);
    }

    @Override
    public void startServer() {
        Configuration serverConfig = getDefaultServerConfiguration();
        serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
        serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
        startServer(serverConfig);
    }
}

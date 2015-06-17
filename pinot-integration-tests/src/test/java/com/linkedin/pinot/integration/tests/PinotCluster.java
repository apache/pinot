package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.BrokerTestUtils;
//import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.server.util.ServerTestUtils;
import org.apache.commons.configuration.Configuration;

import java.io.File;


public class PinotCluster extends ClusterTest {
    static final String ZKString = "localhost:2181";

    public PinotCluster() throws Exception {
//        ZkTestUtils.startLocalZkServer();
        ControllerTestUtils.startController(HELIX_CLUSTER_NAME, ZKString, ControllerTestUtils.getDefaultControllerConfiguration());
        Configuration defaultServerConfiguration = ServerTestUtils.getDefaultServerConfiguration();
        defaultServerConfiguration.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_READ_MODE, "mmap");
        ServerTestUtils.startServer(HELIX_CLUSTER_NAME, ZKString, defaultServerConfiguration);
        BrokerTestUtils.startBroker(HELIX_CLUSTER_NAME, ZKString, BrokerTestUtils.getDefaultBrokerConfiguration());

        // Create a data resource
//        createOfflineResource("MyResource", "DaysSinceEpoch", "daysSinceEpoch", 300, "DAYS");
        addSchema(new File("/Users/Johnny/code/pinot-master/pinot-integration-tests/src/test/resources/chaos-monkey-schema.json"),"myTable");

        // Add table to resource
//        addTableToOfflineResource("MyResource", "MyTable", "DaysSinceEpoch", "daysSinceEpoch");
    }

    public static final String HELIX_CLUSTER_NAME = "ide-test";

    public static void main(String[] args) throws Exception {
        new PinotCluster();

    }

    @Override
    protected String getHelixClusterName() {
        return HELIX_CLUSTER_NAME;
    }
}

package org.apache.pinot.integration.tests;

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManagerTest;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.segment.local.function.GroovySecurityConfigManager;
import org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig;
import org.apache.pinot.spi.utils.StringUtil;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GroovySecurityIntegrationTest {
  private ZkHelixPropertyStore<ZNRecord> _testPropertyStore;
  private HelixManager _helixManager;
  private HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private static final String BROKER_INSTANCE_ID = "broker_instance_1";

  @BeforeTest
  public void beforeTest() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String helixClusterName = "TestGroovyScriptSecurityConfigManager";

    _helixManager = initHelixManager(helixClusterName);
    _testPropertyStore = _helixManager.getHelixPropertyStore();

    _queryQuotaManager =
        new HelixExternalViewBasedQueryQuotaManager(Mockito.mock(BrokerMetrics.class), BROKER_INSTANCE_ID);
    _queryQuotaManager.init(_helixManager);
  }

  @Test
  public void testWritingConfigToPropertyStore() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        null,
        null);
    GroovySecurityConfigManager manager = new GroovySecurityConfigManager(_helixManager);
    manager.setConfig(config);
    GroovyStaticAnalyzerConfig readConfig = manager.getConfig();
  }

  private HelixManager initHelixManager(String helixClusterName) {
    return new GroovySecurityIntegrationTest.FakeHelixManager(helixClusterName, BROKER_INSTANCE_ID, InstanceType.PARTICIPANT,
        _zookeeperInstance.getZkUrl());
  }

  public class FakeHelixManager extends ZKHelixManager {
    FakeHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
      super._zkclient = new ZkClient(StringUtil.join("/", StringUtils.chomp(_zookeeperInstance.getZkUrl(), "/")),
          ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _zkclient.deleteRecursively("/" + clusterName + "/PROPERTYSTORE");
      _zkclient.createPersistent("/" + clusterName + "/PROPERTYSTORE", true);
    }

    void closeZkClient() {
      _zkclient.close();
    }

    @Override
    public HelixAdmin getClusterManagmentTool() {
      return new HelixExternalViewBasedQueryQuotaManagerTest.FakeZKHelixAdmin(_zkclient);
    }
  }
}

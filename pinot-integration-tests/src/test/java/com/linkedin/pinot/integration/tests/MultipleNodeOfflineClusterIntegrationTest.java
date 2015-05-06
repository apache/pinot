package com.linkedin.pinot.integration.tests;

/**
 * TODO Document me!
 *
 * @author jfim
 */
public class MultipleNodeOfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  private static final int BROKER_COUNT = 3;
  private static final int SERVER_COUNT = 5;
  private static final int SERVER_INSTANCE_COUNT = SERVER_COUNT;
  private static final int BROKER_INSTANCE_COUNT = BROKER_COUNT;
  private static final int REPLICA_COUNT = 2;

  @Override
  protected void startCluster() {
    startZk();
    startController();
    startServers(SERVER_COUNT);
    startBrokers(BROKER_COUNT);
  }

  @Override
  protected void createResource() throws Exception {
    createOfflineResource("myresource", "DaysSinceEpoch", "daysSinceEpoch", 3000, "DAYS", SERVER_INSTANCE_COUNT,
        REPLICA_COUNT, BROKER_INSTANCE_COUNT);
  }
}

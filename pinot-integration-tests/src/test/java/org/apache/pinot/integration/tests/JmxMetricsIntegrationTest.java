/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests that verify JMX metrics emitted by various Pinot components.
 */
public class JmxMetricsIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
  private static final String PINOT_JMX_METRICS_DOMAIN = "\"org.apache.pinot.common.metrics\"";
  private static final String BROKER_METRICS_TYPE = "\"BrokerMetrics\"";

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(NUM_BROKERS);
    startServers(NUM_SERVERS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testMultiStageMigrationMetric() throws Exception {
    ObjectName multiStageMigrationMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", BROKER_METRICS_TYPE,
            "name", "\"pinot.broker.singleStageQueriesInvalidMultiStage\"")));

    ObjectName queriesGlobalMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", BROKER_METRICS_TYPE,
            "name", "\"pinot.broker.queriesGlobal\"")));

    // Some queries are run during setup to ensure that all the docs are loaded
    long initialQueryCount = (Long) MBEAN_SERVER.getAttribute(queriesGlobalMetric, "Count");
    assertTrue(initialQueryCount > 0L);
    assertEquals((Long) MBEAN_SERVER.getAttribute(multiStageMigrationMetric, "Count"), 0L);

    postQuery("SELECT COUNT(*) FROM mytable");

    // Run some queries that are known to not work as is with the multi-stage query engine

    // Type differences
    // STRING is VARCHAR in v2
    JsonNode response = postQuery("SELECT CAST(ArrTime AS STRING) FROM mytable");
    assertFalse(response.get("resultTable").get("rows").isEmpty());
    // LONG is BIGINT in v2
    response = postQuery("SELECT CAST(ArrTime AS LONG) FROM mytable");
    assertFalse(response.get("resultTable").get("rows").isEmpty());
    // FLOAT_ARRAY is FLOAT ARRAY in v2
    response = postQuery("SELECT CAST(DivAirportIDs AS FLOAT_ARRAY) FROM mytable");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    // MV column requires ARRAY_TO_MV wrapper to be used in filter predicates
    response = postQuery("SELECT COUNT(*) FROM mytable WHERE DivAirports = 'JFK'");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    // Unsupported function
    response = postQuery("SELECT AirlineID, count(*) FROM mytable WHERE IN_SUBQUERY(airlineID, 'SELECT "
        + "ID_SET(AirlineID) FROM mytable WHERE Carrier = ''AA''') = 1 GROUP BY AirlineID;");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    // Repeated columns in an ORDER BY query
    response = postQuery("SELECT AirTime, AirTime FROM mytable ORDER BY AirTime");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    assertEquals((Long) MBEAN_SERVER.getAttribute(queriesGlobalMetric, "Count"), initialQueryCount + 8L);

    AtomicLong multiStageMigrationMetricValue = new AtomicLong();
    TestUtils.waitForCondition((aVoid) -> {
      try {
        multiStageMigrationMetricValue.set((Long) MBEAN_SERVER.getAttribute(multiStageMigrationMetric, "Count"));
        return multiStageMigrationMetricValue.get() == 6L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 5000, "Expected value of MBean 'pinot.broker.singleStageQueriesInvalidMultiStage' to be: "
        + 6L + "; actual value: " + multiStageMigrationMetricValue.get());

    assertEquals((Long) MBEAN_SERVER.getAttribute(multiStageMigrationMetric, "Count"), 6L);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC, "true");
  }
}

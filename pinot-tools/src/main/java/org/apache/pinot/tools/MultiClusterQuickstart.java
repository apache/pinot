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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.MultiClusterHelixBrokerStarter;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Multi-Cluster Quickstart demonstrating cross-cluster querying via logical tables.
 * Usage: QuickStart -type MULTI_CLUSTER
 */
public class MultiClusterQuickstart extends QuickStartBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterQuickstart.class);
  private static final int NUM_CLUSTERS = 2;
  private static final String LOGICAL_TABLE = "unified_orders";
  private static final String SCHEMA_PATH = "/examples/logicalTables/orders_schema.json";
  private static final String DATA_PATH = "/examples/batch/orders/ordersUS/rawdata/ordersUS_sample.csv";
  private static final int STABILIZATION_PERIOD_MS = 3000; // Wait 3 seconds for count to stabilize

  private final List<Cluster> _clusters = new ArrayList<>();
  private File _tmpDir;

  @Override
  public List<String> types() {
    return List.of("MULTI_CLUSTER", "MULTICLUSTER");
  }

  @Override
  public void execute() throws Exception {
    _tmpDir = _setCustomDataDir ? _dataDir : new File(_dataDir, "multicluster_" + System.currentTimeMillis());
    _tmpDir.mkdirs();

    printStatus(Quickstart.Color.CYAN, "***** Starting Multi-Cluster Quickstart *****");
    try {
      startClusters();
      setupTables();
      runQueries();
      printStatus(Quickstart.Color.GREEN, "***** Multi-Cluster Quickstart Complete *****");
      for (Cluster c : _clusters) {
        printStatus(Quickstart.Color.GREEN, "  " + c._name + ": http://localhost:" + c._controllerPort);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(this::cleanup));
    } catch (Exception e) {
      LOGGER.error("Quickstart failed", e);
      cleanup();
      throw e;
    }
  }

  private void startClusters() throws Exception {
    // Initialize clusters
    for (int i = 0; i < NUM_CLUSTERS; i++) {
      Cluster c = new Cluster();
      c._index = i;
      c._name = "MultiCluster" + (i + 1);
      c._dir = new File(_tmpDir, "cluster" + (i + 1));
      c._dir.mkdirs();
      c._tableName = "orders_cluster" + (i + 1);
      _clusters.add(c);
    }

    // Start ZooKeepers
    printStatus(Quickstart.Color.CYAN, "Starting ZooKeepers...");
    for (Cluster c : _clusters) {
      c._zk = ZkStarter.startLocalZkServer();
      c._zkUrl = c._zk.getZkUrl();
      printStatus(Quickstart.Color.GREEN, "  Started ZK for " + c._name + " at " + c._zkUrl);
    }

    // Start Controllers
    printStatus(Quickstart.Color.CYAN, "Starting Controllers...");
    for (Cluster c : _clusters) {
      c._controllerPort = NetUtils.findOpenPort(9000 + c._index * 100);
      Map<String, Object> cfg = Map.of(
          ControllerConf.ZK_STR, c._zkUrl,
          ControllerConf.HELIX_CLUSTER_NAME, c._name,
          ControllerConf.CONTROLLER_HOST, "localhost",
          ControllerConf.CONTROLLER_PORT, c._controllerPort,
          ControllerConf.DATA_DIR, new File(c._dir, "ctrl").getAbsolutePath(),
          ControllerConf.DISABLE_GROOVY, false,
          CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
      c._controller = new ControllerStarter();
      c._controller.init(new PinotConfiguration(cfg));
      c._controller.start();
      printStatus(Quickstart.Color.GREEN, "  Started Controller for " + c._name + " on port " + c._controllerPort);
    }

    // Start Brokers with multi-cluster config
    printStatus(Quickstart.Color.CYAN, "Starting Brokers...");
    for (Cluster c : _clusters) {
      c._brokerPort = NetUtils.findOpenPort(8000 + c._index * 100);
      PinotConfiguration cfg = new PinotConfiguration();
      cfg.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, c._zkUrl);
      cfg.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, c._name);
      cfg.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, "localhost");
      cfg.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, c._brokerPort);
      cfg.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);

      // Configure remote clusters
      List<String> remotes = new ArrayList<>();
      for (Cluster r : _clusters) {
        if (r != c) {
          remotes.add(r._name);
          cfg.setProperty(String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, r._name), r._zkUrl);
        }
      }
      if (!remotes.isEmpty()) {
        cfg.setProperty(Helix.CONFIG_OF_REMOTE_CLUSTER_NAMES, String.join(",", remotes));
      }

      c._broker = new MultiClusterHelixBrokerStarter();
      c._broker.init(cfg);
      c._broker.start();
      printStatus(Quickstart.Color.GREEN, "  Started Broker for " + c._name + " on port " + c._brokerPort);
    }

    // Start Servers
    printStatus(Quickstart.Color.CYAN, "Starting Servers...");
    for (Cluster c : _clusters) {
      int base = 7000 + c._index * 100;
      c._serverPort = NetUtils.findOpenPort(base);
      PinotConfiguration cfg = new PinotConfiguration();
      cfg.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, c._zkUrl);
      cfg.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, c._name);
      cfg.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, "localhost");
      cfg.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, NetUtils.findOpenPort(base + 10));
      cfg.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, c._serverPort);
      cfg.setProperty(Server.CONFIG_OF_GRPC_PORT, NetUtils.findOpenPort(base + 20));
      cfg.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, new File(c._dir, "data").getAbsolutePath());
      cfg.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, new File(c._dir, "seg").getAbsolutePath());
      cfg.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
      cfg.setProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, true);
      c._server = new HelixServerStarter();
      c._server.init(cfg);
      c._server.start();
      printStatus(Quickstart.Color.GREEN, "  Started Server for " + c._name + " on port " + c._serverPort);
    }

    // Print cluster summary
    printStatus(Quickstart.Color.GREEN, "\n***** All clusters started *****");
    printStatus(Quickstart.Color.YELLOW, "Cluster Summary:");
    for (Cluster c : _clusters) {
      printStatus(Quickstart.Color.YELLOW, "  " + c._name + ":");
      printStatus(Quickstart.Color.YELLOW, "    ZooKeeper:  http://" + c._zkUrl);
      printStatus(Quickstart.Color.YELLOW, "    Controller: http://localhost:" + c._controllerPort);
      printStatus(Quickstart.Color.YELLOW, "    Broker:     http://localhost:" + c._brokerPort);
      printStatus(Quickstart.Color.YELLOW, "    Server:     http://localhost:" + c._serverPort);
    }
  }

  private void setupTables() throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Setting up tables *****");

    // Create physical tables
    for (Cluster c : _clusters) {
      Schema schema = loadSchema(c._tableName);
      uploadSchema(c, schema);
      String tableJson = new TableConfigBuilder(TableType.OFFLINE).setTableName(c._tableName).build().toJsonString();
      post(c.ctrlUrl("/tables"), tableJson);
      printStatus(Quickstart.Color.GREEN, "  Physical table: " + c._tableName + " on " + c._name);
    }

    // Load data
    for (Cluster c : _clusters) {
      File dataFile = new File(c._dir, "data.csv");
      generateClusterData(c, dataFile);
      String url = c.ctrlUrl("/ingestFromFile?tableNameWithType=" + c._tableName + "_OFFLINE"
          + "&batchConfigMapStr=" + java.net.URLEncoder.encode("{\"inputFormat\":\"csv\"}", StandardCharsets.UTF_8));
      var entity = MultipartEntityBuilder.create()
          .addPart("file", new FileBody(dataFile, ContentType.TEXT_PLAIN)).build();
      new HttpClient().sendPostRequest(URI.create(url), entity, null, null);
      printStatus(Quickstart.Color.GREEN, "  Data loaded: " + c._name);
    }

    // Create logical tables
    for (Cluster c : _clusters) {
      Schema schema = loadSchema(LOGICAL_TABLE);
      uploadSchema(c, schema);

      Map<String, PhysicalTableConfig> physTables = new HashMap<>();
      for (Cluster other : _clusters) {
        physTables.put(other._tableName + "_OFFLINE", new PhysicalTableConfig(other != c));
      }
      LogicalTableConfig ltc = new LogicalTableConfig();
      ltc.setTableName(LOGICAL_TABLE);
      ltc.setBrokerTenant("DefaultTenant");
      ltc.setRefOfflineTableName(c._tableName + "_OFFLINE");
      ltc.setPhysicalTableConfigMap(physTables);
      post(c.ctrlUrl("/logicalTables"), ltc.toSingleLineJsonString());
      printStatus(Quickstart.Color.GREEN, "  Logical table on " + c._name);
    }

    waitForTablesReady();
  }

  /**
   * Wait for all tables to be ready by checking table size stabilizes.
   * This replaces the fixed-time sleep with condition-based waiting.
   */
  private void waitForTablesReady() throws Exception {
    printStatus(Quickstart.Color.CYAN, "Waiting for physical tables to be ready...");

    // Wait for each physical table and sum their counts
    long expectedTotal = _clusters.stream()
        .mapToLong(c -> {
          try {
            String sql = "SELECT COUNT(*) FROM " + c._tableName;
            long count = waitForStableCount(c, sql, null);
            printStatus(Quickstart.Color.GREEN, "  " + c._name + "/" + c._tableName + " ready (" + count + " rows)");
            return count;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })
        .sum();

    // Wait for logical table to show aggregated count
    printStatus(Quickstart.Color.CYAN, "Waiting for logical table to be ready...");
    String sql = "SET enableMultiClusterRouting=true; SELECT COUNT(*) FROM " + LOGICAL_TABLE;
    long logicalCount = waitForStableCount(_clusters.get(0), sql, expectedTotal);
    printStatus(Quickstart.Color.GREEN,
        "  Logical table ready: " + logicalCount + " rows (sum of all " + _clusters.size() + " clusters)");
  }

  /**
   * Wait for a table's count to stabilize (remain constant for STABILIZATION_PERIOD_MS).
   * @param cluster the cluster to query
   * @param sql the SQL to execute for counting rows
   * @param expectedCount if not null, wait for count to match this value before stabilizing
   * @return the stable row count
   */
  private long waitForStableCount(Cluster cluster, String sql, Long expectedCount) throws Exception {
    Long lastCount = null;
    long stableStart = 0;
    long deadline = System.currentTimeMillis() + 60 * 1000; // 60 second timeout

    while (System.currentTimeMillis() < deadline) {
      try {
        long currentCount = Long.parseLong(getCount(query(cluster, sql)));
        boolean meetsExpectation = currentCount > 0 && (expectedCount == null || currentCount == expectedCount);

        if (meetsExpectation && lastCount != null && currentCount == lastCount) {
          if (stableStart == 0) {
            stableStart = System.currentTimeMillis();
          } else if (System.currentTimeMillis() - stableStart >= STABILIZATION_PERIOD_MS) {
            return currentCount;
          }
        } else {
          stableStart = 0;
          lastCount = currentCount;
        }
      } catch (Exception e) {
        LOGGER.error("Query failed for {}", sql, e);
      }

      Thread.sleep(1000);
    }

    throw new Exception("Table did not stabilize within the allotted time");
  }

  private void runQueries() throws Exception {
    printStatus(Quickstart.Color.YELLOW, "\n======== SAMPLE QUERIES ========\n");
    Cluster primary = _clusters.get(0);

    // Per-cluster counts
    printStatus(Quickstart.Color.CYAN, "1. Query each cluster's physical table:");
    for (Cluster c : _clusters) {
      String sql = "SELECT COUNT(*) FROM " + c._tableName;
      printStatus(Quickstart.Color.YELLOW, "   SQL: " + sql);
      JsonNode r = query(c, sql);
      printStatus(Quickstart.Color.GREEN, "   Result: " + getCount(r) + " rows\n");
    }

    // Federated query (single-stage)
    printStatus(Quickstart.Color.CYAN, "2. Multi-cluster query (single-stage engine):");
    String fedSql = "SET enableMultiClusterRouting=true; SELECT COUNT(*) FROM " + LOGICAL_TABLE;
    printStatus(Quickstart.Color.YELLOW, "   SQL: " + fedSql);
    JsonNode r = query(primary, fedSql);
    printStatus(Quickstart.Color.GREEN, "   Result: " + getCount(r) + " rows (combined from all clusters)\n");

    // MSE query with GROUP BY
    printStatus(Quickstart.Color.CYAN, "3. Multi-cluster query with GROUP BY (multi-stage engine):");
    String mseSql = "SET enableMultiClusterRouting=true; SET useMultistageEngine=true; "
        + "SELECT region, COUNT(*) FROM " + LOGICAL_TABLE + " GROUP BY region ORDER BY region";
    printStatus(Quickstart.Color.YELLOW, "   SQL: " + mseSql);
    printStatus(Quickstart.Color.GREEN, "   Result:");
    r = query(primary, mseSql);
    for (JsonNode row : r.path("resultTable").path("rows")) {
      printStatus(Quickstart.Color.GREEN, "     " + row.get(0).asText() + ": " + row.get(1).asText() + " rows");
    }

    printStatus(Quickstart.Color.YELLOW, "\nTip: Use 'SET enableMultiClusterRouting=true' for cross-cluster queries");
  }

  private Schema loadSchema(String name) throws Exception {
    try (InputStream is = getClass().getResourceAsStream(SCHEMA_PATH)) {
      Schema s = Schema.fromInputStream(is);
      s.setSchemaName(name);
      return s;
    }
  }

  private void uploadSchema(Cluster c, Schema schema) throws Exception {
    File f = File.createTempFile("schema", ".json");
    f.deleteOnExit();
    FileUtils.writeStringToFile(f, schema.toPrettyJsonString(), StandardCharsets.UTF_8);
    var entity = MultipartEntityBuilder.create()
        .addPart("schema", new FileBody(f, ContentType.APPLICATION_JSON)).build();
    new HttpClient().sendPostRequest(URI.create(c.ctrlUrl("/schemas?override=true&force=true")), entity, null, null);
  }

  private void generateClusterData(Cluster c, File out) throws Exception {
    try (InputStream is = getClass().getResourceAsStream(DATA_PATH)) {
      String[] lines = new String(is.readAllBytes(), StandardCharsets.UTF_8).split("\n");
      List<String> result = new ArrayList<>();
      result.add(lines[0]); // header
      String region = "cluster" + (c._index + 1);
      for (int i = 1; i < lines.length; i++) {
        String[] p = lines[i].split(",");
        if (p.length >= 3) {
          p[0] = region + "_" + p[0];
          p[2] = region;
          result.add(String.join(",", p));
        }
      }
      FileUtils.writeLines(out, result);
    }
  }

  private JsonNode query(Cluster c, String sql) throws Exception {
    String url = "http://localhost:" + c._brokerPort + "/query/sql";
    String resp = post(url, JsonUtils.objectToString(Map.of("sql", sql)));
    return JsonUtils.stringToJsonNode(resp);
  }

  private String getCount(JsonNode r) {
    try {
      return r.path("resultTable").path("rows").get(0).get(0).asText();
    } catch (Exception e) {
      return "N/A";
    }
  }

  private String post(String url, String body) throws Exception {
    return AbstractBaseAdminCommand.sendPostRequest(url, body);
  }

  private void cleanup() {
    for (Cluster c : _clusters) {
      try {
        if (c._server != null) {
          c._server.stop();
        }
      } catch (Exception ignored) {
      }
      try {
        if (c._broker != null) {
          c._broker.stop();
        }
      } catch (Exception ignored) {
      }
      try {
        if (c._controller != null) {
          c._controller.stop();
        }
      } catch (Exception ignored) {
      }
      try {
        if (c._zk != null) {
          ZkStarter.stopLocalZkServer(c._zk);
        }
      } catch (Exception ignored) {
      }
    }
    try {
      FileUtils.deleteDirectory(_tmpDir);
    } catch (Exception ignored) {
    }
  }

  private static class Cluster {
    int _index;
    String _name;
    File _dir;
    String _tableName;
    ZkStarter.ZookeeperInstance _zk;
    String _zkUrl;
    BaseControllerStarter _controller;
    int _controllerPort;
    BaseBrokerStarter _broker;
    int _brokerPort;
    BaseServerStarter _server;
    int _serverPort;

    String ctrlUrl(String path) {
      return "http://localhost:" + _controllerPort + path;
    }
  }

  public static void main(String[] args) throws Exception {
    new MultiClusterQuickstart().execute();
  }
}

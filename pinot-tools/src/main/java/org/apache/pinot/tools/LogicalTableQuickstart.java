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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.apache.pinot.tools.admin.command.QuickstartRunner;

/**
 * Quick start for Logical Tables.
 *
 * This quickstart demonstrates how to create and query logical tables that provide
 * a unified view over multiple physical tables. In this example, we create three
 * physical tables (ordersUS_OFFLINE, ordersEU_OFFLINE, and ordersAPAC_OFFLINE)
 * and then create a logical table (orders) that allows querying all tables as if
 * they were one. Each physical table is regionally partitioned by the region column
 * (us, eu, or apac). All orders tables are organized under examples/batch/orders/.
 *
 * Logical tables are useful for:
 * - Creating unified views over similar tables with the same schema
 * - Implementing table partitioning strategies (regional partitioning in this case)
 * - Providing abstraction layers over complex table hierarchies
 * - Time-based or geography-based table splitting
 */
public class LogicalTableQuickstart extends Quickstart {

  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{
        "examples/batch/orders/ordersUS",
        "examples/batch/orders/ordersEU",
        "examples/batch/orders/ordersAPAC"
    };
  }

  public void execute() throws Exception {
    File quickstartTmpDir = _setCustomDataDir ? _dataDir : new File(_dataDir,
        String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());

    // Bootstrap physical tables
    Set<QuickstartTableRequest> quickstartTableRequests = new HashSet<>();
    quickstartTableRequests.addAll(bootstrapOfflineTableDirectories(quickstartTmpDir));

    final QuickstartRunner runner = new QuickstartRunner(new ArrayList<>(quickstartTableRequests), 1, 1, 1, 1,
        quickstartRunnerDir, getConfigOverrides());

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down logical table quickstart *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.CYAN, "***** Bootstrapping physical tables *****");
    runner.bootstrapTable();
    printStatus(Color.CYAN, "***** Creating logical table *****");
    createLogicalTable(runner);
    printStatus(Color.CYAN, "***** Waiting for 5 seconds for tables to be loaded *****");
    Thread.sleep(5000);
    printStatus(Color.YELLOW, "***** Logical table quickstart setup complete *****");
    runSampleQueries(runner);
    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  private void createLogicalTable(QuickstartRunner runner) throws Exception {
    createLogicalTableSchema();

    LogicalTableConfig logicalTableConfig = new LogicalTableConfig();
    logicalTableConfig.setTableName("orders");
    logicalTableConfig.setBrokerTenant("DefaultTenant");
    logicalTableConfig.setRefOfflineTableName("ordersUS_OFFLINE");

    Map<String, PhysicalTableConfig> physicalTableConfigMap = Map.of(
        "ordersUS_OFFLINE", new PhysicalTableConfig(),
        "ordersEU_OFFLINE", new PhysicalTableConfig(),
        "ordersAPAC_OFFLINE", new PhysicalTableConfig()
    );
    logicalTableConfig.setPhysicalTableConfigMap(physicalTableConfigMap);

    String logicalTableUrl = "http://localhost:" + QuickstartRunner.DEFAULT_CONTROLLER_PORT + "/logicalTables";

    try {
      String response = AbstractBaseAdminCommand.sendPostRequest(
          logicalTableUrl, logicalTableConfig.toSingleLineJsonString());
      printStatus(Color.GREEN, "***** Logical table created successfully *****");
    } catch (Exception e) {
      printStatus(Color.YELLOW, "***** Logical table creation failed: " + e.getMessage() + " - continuing with physical tables *****");
    }
  }

  private void createLogicalTableSchema() throws Exception {
    String schemaResourcePath = "/examples/logicalTables/orders_schema.json";

    try (InputStream inputStream = getClass().getResourceAsStream(schemaResourcePath)) {
      if (inputStream == null) {
        throw new RuntimeException("Schema file not found: " + schemaResourcePath);
      }

      String schemaJsonString = IOUtils.toString(inputStream, "UTF-8");
      Schema logicalTableSchema = Schema.fromString(schemaJsonString);
      printStatus(Color.CYAN, "***** Loaded logical table schema: " + logicalTableSchema.getSchemaName() + " *****");

      // Create schema via multipart form data
      File tempSchemaFile = File.createTempFile("orders_schema", ".json");
      tempSchemaFile.deleteOnExit();
      FileUtils.writeStringToFile(tempSchemaFile, schemaJsonString, StandardCharsets.UTF_8);
      HttpEntity multipartEntity = MultipartEntityBuilder.create()
          .addPart("schema", new FileBody(tempSchemaFile, ContentType.APPLICATION_JSON,
              logicalTableSchema.getSchemaName() + ".json"))
          .build();
      HttpClient httpClient = new HttpClient();
      String schemaUrl = "http://localhost:" + QuickstartRunner.DEFAULT_CONTROLLER_PORT + "/schemas?override=true&force=true";

      SimpleHttpResponse response = httpClient.sendPostRequest(
          java.net.URI.create(schemaUrl), multipartEntity, null, null);
      if (response.getStatusCode() == 200) {
        printStatus(Color.GREEN, "***** Logical table schema created successfully *****");
      } else {
        printStatus(Color.YELLOW, "***** Schema creation response: " + response.getResponse() + " *****");
      }
    } catch (Exception e) {
      printStatus(Color.YELLOW, "***** Schema creation failed: " + e.getMessage() + " - continuing anyway *****");
    }
  }

  public void runSampleQueries(QuickstartRunner runner) throws Exception {
    printStatus(Color.YELLOW, "***** Running sample queries on physical tables *****");

    runAndPrintQuery(runner, "SELECT COUNT(*) FROM ordersUS_OFFLINE");
    runAndPrintQuery(runner, "SELECT COUNT(*) FROM ordersEU_OFFLINE");
    runAndPrintQuery(runner, "SELECT COUNT(*) FROM ordersAPAC_OFFLINE");

    printStatus(Color.YELLOW, "***** Running sample queries on logical table *****");

    runAndPrintQuery(runner, "SELECT COUNT(*) FROM orders");
    runAndPrintQuery(runner, "SELECT orderId, customerId, region, productId, status FROM orders WHERE region = 'us' LIMIT 10");
    runAndPrintQuery(runner, "SELECT region, COUNT(*) as orderCount FROM orders GROUP BY region ORDER BY region");
    runAndPrintQuery(runner, "SELECT status, COUNT(*) as count FROM orders GROUP BY status ORDER BY status");

    printStatus(Color.GREEN, "***** Logical Table Demo Complete! *****");
    printStatus(Color.GREEN, "***** The logical table 'orders' provides a unified view over all regional tables *****");
  }

  private void runAndPrintQuery(QuickstartRunner runner, String query) throws Exception {
    printStatus(Color.YELLOW, "Query: " + query);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
  }

  public static void main(String[] args) throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "LOGICAL_TABLE"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  @Override
  public List<String> types() {
    return Arrays.asList("LOGICAL_TABLE");
  }
}

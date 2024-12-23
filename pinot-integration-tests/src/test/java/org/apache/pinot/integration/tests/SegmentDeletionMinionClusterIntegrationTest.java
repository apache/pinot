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
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.File;
import java.util.Collection;
import java.util.LinkedHashSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentDeletionMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionMinionClusterIntegrationTest.class);

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();
  }

  @AfterClass
  public void tearDown() {
    try {
      stopMinion();
      stopServer();
      stopBroker();
      stopController();
      stopZk();
    } finally {
      FileUtils.deleteQuietly(_tempDir);
    }
  }

  private int getTotalDocs(String tableName)
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + tableName;
    JsonNode response = postQuery(query);
    JsonNode resTbl = response.get("resultTable");
    return (resTbl == null) ? 0 : resTbl.get("rows").get(0).get(0).asInt();
  }

  private Collection<String> getSegmentNames(String tableName) {
    try {
      String query = "SELECT $segmentName FROM " + tableName;
      JsonNode response = postQuery(query);
      JsonNode resTbl = response.get("resultTable");
      Collection<String> segmentNames = new LinkedHashSet<>();
      if (resTbl == null) {
        return segmentNames;
      }
      ArrayNode rowsArray = (ArrayNode) resTbl.get("rows");
      if (rowsArray == null) {
        return segmentNames;
      }
      rowsArray.forEach(row -> {
        ArrayNode oneRow = (ArrayNode) row;
        segmentNames.add(oneRow.get(0).asText());
      });
      return segmentNames;
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private boolean deleteSegmentEqualToName(String tableName, String segmentName, String taskName)
      throws Exception {

    String deleteFromStatement =
        String.format("DELETE FROM %s WHERE $segmentName = '%s' OPTION(taskName=%s)",
            tableName, segmentName, taskName);

    JsonNode response = postQueryToController(deleteFromStatement);
    JsonNode resTbl = response.get("resultTable");
    if (resTbl == null) {
      return false;
    } else {
      ArrayNode rows = (ArrayNode) resTbl.get("rows");
      if (rows == null) {
        return false;
      } else {
        return (rows.size() == 1);
      }
    }
  }

  private boolean deleteSegmentLikeName(String tableName, String segmentNamePattern, String taskName)
      throws Exception {

    String deleteFromStatement =
        String.format("DELETE FROM %s WHERE $segmentName LIKE '%s' OPTION(taskName=%s)",
            tableName, segmentNamePattern, taskName);

    JsonNode response = postQueryToController(deleteFromStatement);
    JsonNode resTbl = response.get("resultTable");
    if (resTbl == null) {
      return false;
    } else {
      ArrayNode rows = (ArrayNode) resTbl.get("rows");
      if (rows == null) {
        return false;
      } else {
        return (rows.size() == 1);
      }
    }
  }

  @Test
  public void testDeleteSegmentEqualNameSqlToController()
      throws Exception {

    String tableName = "testDeleteSegmentEqualNameSqlToController";

    testInsertIntoFromFile(tableName, tableName + "_InsertInto_Task", true);

    final int totalDocsBeforeDelete = getTotalDocs(tableName);
    Assert.assertEquals(totalDocsBeforeDelete, 70);

    Collection<String> segmentNames = getSegmentNames(tableName);

    final int segmentCountBeforeDelete = segmentNames.size();
    Assert.assertEquals(segmentCountBeforeDelete, 1);

    final String segmentToDelete = segmentNames.iterator().next();

    final String deleteSegmentTaskName = tableName + "_DeleteFrom_Task";

    Assert.assertTrue(deleteSegmentEqualToName(tableName, segmentToDelete, deleteSegmentTaskName));

    TestUtils.waitForCondition(aVoid -> {
        int segmentCountAfterDelete = getSegmentNames(tableName).size();
        return segmentCountAfterDelete < segmentCountBeforeDelete;
        },
        3000L,
        30_000L,
        String.format("segmentCountAfterDelete never dropped below segmentCountBeforeDelete (%s)",
            segmentCountBeforeDelete),
        true);
  }

  @Test
  public void testDeleteSegmentLikeNameSqlToController()
      throws Exception {

    String tableName = "testDeleteSegmentLikeNameSqlToController";

    testInsertIntoFromFile(tableName, tableName + "_InsertInto_Task", true);

    final int totalDocsBeforeDelete = getTotalDocs(tableName);
    Assert.assertEquals(totalDocsBeforeDelete, 70);

    Collection<String> segmentNames = getSegmentNames(tableName);

    final int segmentCountBeforeDelete = segmentNames.size();
    Assert.assertEquals(segmentCountBeforeDelete, 1);

    final String segmentToDelete = segmentNames.iterator().next();

    final String deleteSegmentTaskName = tableName + "_DeleteFrom_Task";

    Assert.assertTrue(deleteSegmentLikeName(tableName, segmentToDelete, deleteSegmentTaskName));

    TestUtils.waitForCondition(aVoid -> {
          int segmentCountAfterDelete = getSegmentNames(tableName).size();
          return segmentCountAfterDelete < segmentCountBeforeDelete;
        },
        3000L,
        30_000L,
        String.format("segmentCountAfterDelete never dropped below segmentCountBeforeDelete (%s)",
            segmentCountBeforeDelete),
        true);
  }

  private void testInsertIntoFromFile(String tableName, String taskName, boolean queryController)
      throws Exception {
    addSchemaAndTableConfig(tableName);

    File inputDir = new File(_tempDir, tableName);
    int rowCnt = prepInputFiles(inputDir, 7, 10);
    assertEquals(rowCnt, 70);

    String insertFileStatement =
        String.format("INSERT INTO %s FROM FILE '%s' OPTION(taskName=%s)", tableName, inputDir.getAbsolutePath(),
            taskName);
    TestUtils.waitForCondition(aVoid -> {
      try {
        if (getTotalDocs(tableName) < rowCnt) {
          JsonNode response = queryController ? postQueryToController(insertFileStatement)
              : postQuery(insertFileStatement);
          Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asText(),
              tableName + "_OFFLINE");
          Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(1).asText(),
              "Task_SegmentGenerationAndPushTask_" + taskName);
        }
        return getTotalDocs(tableName) == rowCnt;
      } catch (Exception e) {
        LOGGER.error("Failed to get expected totalDocs: {}", rowCnt, e);
        return false;
      }
    }, 5000L, 600_000L, "Failed to load " + rowCnt + " documents", true);
    JsonNode result = postQuery("SELECT COUNT(*) FROM " + tableName);
    // One segment per file.
    assertEquals(result.get("numSegmentsQueried").asInt(), 7);
  }

  private void addSchemaAndTableConfig(String tableName)
      throws Exception {
    addSchema(new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING).build());
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toString(),
        BasicAuthTestUtils.AUTH_HEADER);
  }

  private int prepInputFiles(File inputDir, int fileNum, int rowsPerFile)
      throws Exception {
    int rowCnt = 0;
    for (int i = 0; i < fileNum; i++) {
      File csvFile = new File(inputDir, String.format("tempFile_%05d.csv", i));
      FileUtils.write(csvFile, "id,name\n", false);
      for (int j = 0; j < rowsPerFile; j++) {
        FileUtils.write(csvFile, String.format("%d,n%d\n", rowCnt, rowCnt), true);
        rowCnt++;
      }
    }
    return rowCnt;
  }
}

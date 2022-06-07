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
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OfflineSegmentCreationMultiValueIndexingTest extends BaseClusterIntegrationTestSet {

  @Override
  public String getTableName() {
    return "OFFLINE_MULTIVALUE_TABLE";
  }

  @Override
  protected String getAvroTarFileName() {
    return "offlineMultiValueData.tar.gz";
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 5;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Add schema and table
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("OFFLINE_MULTIVALUE_SCHEMA")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMultiValueDimension("interests", FieldSpec.DataType.STRING)
        .addMultiValueDimension("scores", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setSchemaName("OFFLINE_MULTIVALUE_SCHEMA")
        .setBloomFilterColumns(List.of("interests"))
        .setRangeIndexColumns(List.of("scores"))
        .build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toString(),
        BasicAuthTestUtils.AUTH_HEADER);

    // Ingest data
    List<File> jsonFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentFromJson(jsonFiles.get(0), tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.OFFLINE, _tarDir);
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testMvBloomFilterColumnCount()
      throws Exception {
    JsonNode queryResponse = postQuery("SELECT COUNT(*) FROM OFFLINE_MULTIVALUE_TABLE WHERE interests = 'i1'");
    Assert.assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asLong(), 3);

    queryResponse = postQuery("SELECT COUNT(*) FROM OFFLINE_MULTIVALUE_TABLE WHERE interests = 'i2'");
    Assert.assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asLong(), 1);
  }

  @Test
  public void testMvRangeIndexColumnCount()
      throws Exception {
    JsonNode queryResponse = postQuery("SELECT COUNT(*) FROM OFFLINE_MULTIVALUE_TABLE WHERE scores > 5");
    Assert.assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asLong(), 4);

    queryResponse = postQuery("SELECT COUNT(*) FROM OFFLINE_MULTIVALUE_TABLE WHERE scores < 2");
    Assert.assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asLong(), 2);
  }

}

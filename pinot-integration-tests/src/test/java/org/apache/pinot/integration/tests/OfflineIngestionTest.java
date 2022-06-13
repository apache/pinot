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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class OfflineIngestionTest extends BaseClusterIntegrationTestSet {

  @Override
  protected String getSchemaFileName() {
    return "offlineIngestionTest.schema";
  }

  @Override
  protected String getTableName() {
    return "offlineIngestionTestTable";
  }

  @Override
  protected String getAvroTarFileName() {
    return DEFAULT_AVRO_TAR_FILE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return 100;
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

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName())
        .setRangeIndexColumns(List.of("intSV", "intMV", "longSV", "longMV", "floatSV", "floatMV", "doubleSV",
            "doubleMV"))
        .setBloomFilterColumns(List.of("stringSV", "stringMV"))
        .setNoDictionaryColumns(List.of("intMV", "longMV", "floatMV", "stringMV", "doubleMV"))
        .build();
    addTableConfig(tableConfig);

    // Unpack the json files
    List<File> avroFiles = new ArrayList<>();
    File avroFile = new File(OfflineIngestionTest.class.getClassLoader()
        .getResource("offlineIngestionTestData.avro").toURI());
    avroFiles.add(avroFile);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir,
        _tarDir);
    List<File> tarDirs = new ArrayList<>();
    tarDirs.add(_tarDir);
    try {
      uploadSegments(getTableName(), TableType.OFFLINE, tarDirs);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the two exception - 409 conflict of the second call enters ProcessExistingSegment ; segmentZkMetadata
      // creation failure if both calls entered ProcessNewSegment. In/such cases ensure that we upload all the
      // segments again/to ensure that the data is setup correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment"), e.getMessage());
      uploadSegments(getTableName(), _tarDir);
    }
    // Wait for all documents loaded
    setUpH2Connection(avroFiles);
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testIntSVRangeQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE intSV > 1 AND intSV < 6",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE intSV > 1 AND intSV < 6");
  }

  @Test
  public void testIntMVRangeQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE intMV > 3 AND intMV < 6",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE (intMV__MV0 > 3 AND intMV__MV0 < 6)"
            + " OR (intMV__MV1 > 3 AND intMV__MV1 < 6) OR (intMV__MV2 > 3 AND intMV__MV2 < 6)"
            + " OR (intMV__MV3 > 3 AND intMV__MV3 < 6) OR (intMV__MV4 > 3 AND intMV__MV4 < 6)");
  }

  @Test
  public void testLongSVRangeQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE longSV > 1 AND longSV < 6",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE longSV > 1 AND longSV < 6");
  }

  @Test
  public void testLongMVRangeQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE longMV > 3 AND longMV < 6",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE (longMV__MV0 > 3 AND longMV__MV0 < 6)"
            + " OR (longMV__MV1 > 3 AND longMV__MV1 < 6) OR (longMV__MV2 > 3 AND longMV__MV2 < 6)"
            + " OR (longMV__MV3 > 3 AND longMV__MV3 < 6) OR (longMV__MV4 > 3 AND longMV__MV4 < 6)");
  }

  @Test
  public void testStringSVBloomFilterSelectQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE stringSV = 'str1'",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE stringSV = 'str1'");
  }

  @Test
  public void testStringMVBloomFilterSelectQuery()
      throws Exception {
    testQuery("SELECT count(*) FROM offlineIngestionTestTable WHERE stringMV = 'str1'",
        "SELECT count(*) FROM offlineIngestionTestTable WHERE stringMV__MV0 = 'str1' OR stringMV__MV1 = 'str1'"
            + " OR stringMV__MV2 = 'str1' OR stringMV__MV3 = 'str1' OR stringMV__MV4 = 'str1'");
  }
}

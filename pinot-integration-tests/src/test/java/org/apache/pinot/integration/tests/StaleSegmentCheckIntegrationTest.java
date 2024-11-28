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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.api.resources.TableStaleSegmentResponse;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class StaleSegmentCheckIntegrationTest extends BaseClusterIntegrationTest {
  private static final String JSON_FIELD = "jsonField";

  private PinotTaskManager _taskManager;
  private PinotHelixTaskResourceManager _taskResourceManager;
  private TableConfig _tableConfig;
  private Schema _schema;
  private List<File> _avroFiles;
  private static final String H3_INDEX_COLUMN = "h3Column";
  private static final Map<String, String> H3_INDEX_PROPERTIES = Collections.singletonMap("resolutions", "5");
  private static final String TEXT_INDEX_COLUMN = "textColumn";
  private static final String NULL_INDEX_COLUMN = "nullField";

  private static final String JSON_INDEX_COLUMN = "jsonField";
  private static final String FST_TEST_COLUMN = "DestCityName";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();
    // Start Kafka
    startKafka();

    _taskManager = _controllerStarter.getTaskManager();
    _taskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    _schema = createSchema();
    _schema.addField(new DimensionFieldSpec(JSON_FIELD, FieldSpec.DataType.STRING, true));
    _schema.addField(new DimensionFieldSpec(NULL_INDEX_COLUMN, FieldSpec.DataType.STRING, true));
    _schema.addField(new DimensionFieldSpec(H3_INDEX_COLUMN, FieldSpec.DataType.BYTES, true));
    _schema.addField(new DimensionFieldSpec(TEXT_INDEX_COLUMN, FieldSpec.DataType.STRING, true));

    addSchema(_schema);

    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setTimeColumnName(getTimeColumnName())
            .setIngestionConfig(getIngestionConfig()).setNullHandlingEnabled(true)
            .setNoDictionaryColumns(Collections.singletonList(TEXT_INDEX_COLUMN)).build();
    addTableConfig(_tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(_avroFiles, _tableConfig, _schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(300_000L);
  }

  private FieldConfig getH3FieldConfig() {
    return new FieldConfig(H3_INDEX_COLUMN, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.H3, null,
        H3_INDEX_PROPERTIES);
  }

  private FieldConfig getTextFieldConfig() {
    return new FieldConfig(TEXT_INDEX_COLUMN, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null);
  }

  private FieldConfig getFstFieldConfig() {
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);
    return new FieldConfig(FST_TEST_COLUMN, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null,
        propertiesMap);
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    List<TransformConfig> transforms = new ArrayList<>();
    transforms.add(new TransformConfig(JSON_INDEX_COLUMN,
        "Groovy({'{\"DestState\":\"'+DestState+'\",\"OriginState\":\"'+OriginState+'\"}'}, DestState, OriginState)"));
    transforms.add(new TransformConfig(NULL_INDEX_COLUMN, "Groovy({null})"));
    // This is the byte encoding of ST_POINT(-122, 37)
    transforms.add(new TransformConfig(H3_INDEX_COLUMN,
        "Groovy({[0x00,0xc0,0x5e,0x80,0x00,0x00,0x00,0x00,0x00,0x40,0x42,0x80,0x00,0x00,0x00,0x00,0x00] as byte[]})"));
    transforms.add(new TransformConfig(TEXT_INDEX_COLUMN, "Groovy({\"Hello this is a text column\"})"));

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transforms);

    return ingestionConfig;
  }

  @Test
  public void testAddRemoveSortedIndex()
      throws Exception {
    // Add a sorted column to the table
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    indexingConfig.setSortedColumn(Collections.singletonList("Carrier"));
    updateTableConfig(_tableConfig);

    Map<String, TableStaleSegmentResponse> needRefreshResponses = getStaleSegmentsResponse();
    assertEquals(needRefreshResponses.size(), 1);
    assertEquals(needRefreshResponses.values().iterator().next().getStaleSegmentList().size(), 12);
  }

  @Test(dependsOnMethods = "testAddRemoveSortedIndex")
  public void testAddRemoveRawIndex()
      throws Exception {
    // Add a raw index column
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    indexingConfig.setNoDictionaryColumns(Collections.singletonList("ActualElapsedTime"));
    updateTableConfig(_tableConfig);

    Map<String, TableStaleSegmentResponse> needRefreshResponses = getStaleSegmentsResponse();
    assertEquals(needRefreshResponses.size(), 1);
    assertEquals(needRefreshResponses.values().iterator().next().getStaleSegmentList().size(), 12);
  }

  @Test(dependsOnMethods = "testAddRemoveSortedIndex")
  public void testH3IndexChange()
      throws Exception {
    // Add a H3 index column
    _tableConfig.setFieldConfigList(Collections.singletonList(getH3FieldConfig()));
    updateTableConfig(_tableConfig);

    Map<String, TableStaleSegmentResponse> needRefreshResponses = getStaleSegmentsResponse();
    assertEquals(needRefreshResponses.size(), 1);
    assertEquals(needRefreshResponses.values().iterator().next().getStaleSegmentList().size(), 12);
  }

  private Map<String, TableStaleSegmentResponse> getStaleSegmentsResponse()
      throws IOException {
    return JsonUtils.stringToObject(sendGetRequest(
            _controllerRequestURLBuilder.forStaleSegments(
                TableNameBuilder.OFFLINE.tableNameWithType(getTableName()))),
        new TypeReference<Map<String, TableStaleSegmentResponse>>() { });
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
}

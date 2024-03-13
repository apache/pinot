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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPEncodingRealtimeIntegrationTest extends BaseClusterIntegrationTestSet {
  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _avroFiles = unpackAvroData(_tempDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(1);

    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return Collections.singletonList("logLine");
  }

  @Test
  public void testValues()
      throws Exception {
    Assert.assertEquals(getPinotConnection().execute(
            "SELECT count(*) FROM " + getTableName() + " WHERE REGEXP_LIKE(logLine, '.*executor.*')").getResultSet(0)
        .getLong(0), 53);
  }

  protected int getRealtimeSegmentFlushSize() {
    return 30;
  }

  @Override
  protected long getCountStarResult() {
    return 100;
  }

  @Override
  protected String getTableName() {
    return "clpEncodingIT";
  }

  @Override
  protected String getAvroTarFileName() {
    return "clpEncodingITData.tar.gz";
  }

  @Override
  protected String getSchemaFileName() {
    return "clpEncodingRealtimeIntegrationTestSchema.schema";
  }

  @Override
  protected String getTimeColumnName() {
    return "timestampInEpoch";
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(
        new FieldConfig("logLine", FieldConfig.EncodingType.RAW, null, null, FieldConfig.CompressionCodec.CLP, null,
            null, null, null));

    return fieldConfigs;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    List<TransformConfig> transforms = new ArrayList<>();
    transforms.add(new TransformConfig("timestampInEpoch", "now()"));

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transforms);

    return ingestionConfig;
  }
}

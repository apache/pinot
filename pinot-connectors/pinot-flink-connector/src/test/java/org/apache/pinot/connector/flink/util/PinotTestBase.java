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
package org.apache.pinot.connector.flink.util;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;


public class PinotTestBase extends BaseClusterIntegrationTest {

  public static final List<Row> TEST_DATA = new ArrayList<>();
  public static final RowTypeInfo TEST_TYPE_INFO =
      new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
  public static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.INT)
      .addSingleValueDimension("b", FieldSpec.DataType.LONG).addSingleValueDimension("c", FieldSpec.DataType.STRING)
      .setPrimaryKeyColumns(Lists.newArrayList("a")).build();

  static {
    TEST_DATA.add(Row.of(1, 1L, "Hi"));
    TEST_DATA.add(Row.of(2, 2L, "Hello"));
    TEST_DATA.add(Row.of(3, 3L, "Hello world"));
    TEST_DATA.add(Row.of(4, 4L, "Hello world!"));
    TEST_DATA.add(Row.of(5, 4L, "HelloWorld"));
    TEST_DATA.add(Row.of(6, 5L, "Hello!world!"));
  }

  public void startCluster()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @Override
  @Nullable
  protected IngestionConfig getIngestionConfig() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, getTarDir().getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigMap.put(BatchConfigProperties.PUSH_CONTROLLER_URI, getControllerBaseApiUrl());
    return new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(batchConfigMap), "APPEND", "HOURLY"), null,
        null, null, null);
  }

  public void stopCluster()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  public File getTempDir() {
    return _tempDir;
  }

  public File getSegmentDir() {
    return _segmentDir;
  }

  public File getTarDir() {
    return _tarDir;
  }

  public String getControllerBaseApiUrl() {
    return _controllerBaseApiUrl;
  }

  @Override
  public String getTableName() {
    return super.getTableName();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return super.createOfflineTableConfig();
  }

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    super.addTableConfig(tableConfig);
  }

  public TableConfig getOfflineTableConfig(String tableName) {
    return super.getOfflineTableConfig(tableName);
  }

  public ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }

  @Override
  public void dropOfflineTable(String tableNameWithType)
      throws IOException {
    super.dropOfflineTable(tableNameWithType);
  }
}

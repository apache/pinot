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
package org.apache.pinot.integration.tests.custom;

import java.io.File;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;


/// Same as [JsonExtractIndexGroupByTest] but against a realtime table, so the JSON index being read is the mutable
/// (consuming segment) implementation. The production table in PINOT-489 is realtime.
@Test(suiteName = "CustomClusterIntegrationTest")
public class JsonExtractIndexGroupByRealtimeTest extends JsonExtractIndexGroupByTest {
  private static final String TABLE_NAME = "RTJsonExtractIndexGroupByTest";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // Keep everything in consuming (mutable) segments so the mutable JSON index is exercised.
    return 1_000_000;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName())
        .setStreamConfigs(getStreamConfigs())
        .setTimeColumnName(getTimeColumnName())
        .setJsonIndexColumns(List.of(PROPERTIES_FIELD))
        .setNoDictionaryColumns(List.of(PROPERTIES_FIELD))
        .setNumReplicas(getNumReplicas())
        .build();
  }
}

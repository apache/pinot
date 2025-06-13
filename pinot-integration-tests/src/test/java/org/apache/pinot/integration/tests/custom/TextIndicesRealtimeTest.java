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


// same as TextIndicesTest, but with a realtime table
public class TextIndicesRealtimeTest extends TextIndicesTest {
  private static final String TABLE_NAME = "RTTextIndicesTest";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(getTableName())
        .setIngestionConfig(null)
        .setStreamConfigs(getStreamConfigs())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  protected List<String> getInvertedIndexColumns() {
    return List.of();
  }
}

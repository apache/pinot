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
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


public class MultiColumnRealtimeRowMajorTextIndicesTest extends MultiColumnTextIndicesTest {

  private static final String TABLE_NAME = "MultiColRTTextIndicesRowMajorTest";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    if (sampleAvroFile != null) {
      AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    }

    Map<String, String> streamConfigs = getStreamConfigs();
    IngestionConfig ingestionCfg = new IngestionConfig();
    StreamIngestionConfig streamCfg = new StreamIngestionConfig(List.of(streamConfigs));
    streamCfg.setColumnMajorSegmentBuilderEnabled(false);
    ingestionCfg.setStreamIngestionConfig(streamCfg);

    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(getTableName())
        .setMultiColumnTextIndexConfig(getMultiColumnTextIndexConfig())
        .setIngestionConfig(ingestionCfg)
        .setStreamConfigs(null)
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

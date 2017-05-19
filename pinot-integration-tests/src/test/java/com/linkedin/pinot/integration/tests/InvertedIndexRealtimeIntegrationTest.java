/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import java.io.File;
import java.util.List;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;
/**
 * enables indexes on a bunch of columns 
 *
 */
@Test
public class InvertedIndexRealtimeIntegrationTest extends RealtimeClusterIntegrationTest {

  @Override
  protected void addRealtimeTable(String tableName, String timeColumnName, String timeColumnType, int retentionDays,
      String retentionTimeUnit, String kafkaZkUrl, String kafkaTopic, String schemaName, String serverTenant,
      String brokerTenant, File avroFile, int realtimeSegmentFlushSize, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode, List<String> noDictionaryColumns) throws Exception {
    Schema schema = Schema.fromFile(getSchemaFile());
    super.addRealtimeTable(tableName, timeColumnName, timeColumnType, retentionDays, retentionTimeUnit, kafkaZkUrl,
        kafkaTopic, schemaName, serverTenant, brokerTenant, avroFile, realtimeSegmentFlushSize, sortedColumn,
        schema.getDimensionNames(), null, null);
  }
}

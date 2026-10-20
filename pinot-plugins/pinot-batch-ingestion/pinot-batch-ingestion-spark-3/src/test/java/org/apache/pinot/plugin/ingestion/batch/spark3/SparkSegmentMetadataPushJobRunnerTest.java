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
package org.apache.pinot.plugin.ingestion.batch.spark3;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;


/// Verifies that empty selections retain consistent REFRESH replacement without starting Spark uploads.
public class SparkSegmentMetadataPushJobRunnerTest {
  @DataProvider
  public Object[][] emptyPushModes() {
    return new Object[][]{{true, 0}, {true, 1}, {true, 2}, {false, 0}};
  }

  @Test(dataProvider = "emptyPushModes")
  public void testEmptySelectionHonorsConsistentPush(boolean consistentPush, int parallelism)
      throws Exception {
    Path outputDir = Files.createTempDirectory("empty-metadata-push-");
    try {
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", consistentPush));
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test")
          .setIngestionConfig(ingestion).build();
      Path tableConfigFile = outputDir.resolve("table.json");
      Files.writeString(tableConfigFile, tableConfig.toJsonString());
      TableSpec tableSpec = new TableSpec();
      tableSpec.setTableName("test");
      tableSpec.setTableConfigURI(tableConfigFile.toUri().toString());
      SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
      spec.setTableSpec(tableSpec);
      spec.setOutputDirURI(outputDir.toUri().toString());
      spec.setPinotFSSpecs(List.of());
      PushJobSpec pushSpec = new PushJobSpec();
      pushSpec.setPushParallelism(parallelism);
      spec.setPushJobSpec(pushSpec);
      Map<URI, String> lineage = Map.of(URI.create("http://localhost:9000"), "lineage");
      try (MockedStatic<ConsistentDataPushUtils> consistent = mockStatic(ConsistentDataPushUtils.class)) {
        consistent.when(() -> ConsistentDataPushUtils.consistentDataPushEnabled(any())).thenCallRealMethod();
        consistent.when(() -> ConsistentDataPushUtils.preUpload(spec, List.of())).thenReturn(lineage);
        SparkSegmentMetadataPushJobRunner runner = new SparkSegmentMetadataPushJobRunner();
        runner.init(spec);
        runner.run();
        consistent.verify(() -> ConsistentDataPushUtils.preUpload(spec, List.of()), times(consistentPush ? 1 : 0));
        consistent.verify(() -> ConsistentDataPushUtils.postUpload(spec, lineage), times(consistentPush ? 1 : 0));
      }
    } finally {
      FileUtils.deleteDirectory(outputDir.toFile());
    }
  }
}

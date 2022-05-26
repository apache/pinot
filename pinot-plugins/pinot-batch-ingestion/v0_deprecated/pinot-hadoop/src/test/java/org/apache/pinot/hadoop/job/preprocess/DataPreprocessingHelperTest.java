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
package org.apache.pinot.hadoop.job.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class DataPreprocessingHelperTest {

  @Test
  public void testDataPreprocessingHelper()
      throws IOException {
    List<Path> inputPaths = new ArrayList<>();
    String pathString = Preconditions
        .checkNotNull(DataPreprocessingHelperTest.class.getClassLoader().getResource("data/test_sample_data.avro"))
        .getPath();
    inputPaths.add(new Path(pathString));
    Path outputPath = new Path("mockOutputPath");
    DataPreprocessingHelper dataPreprocessingHelper = new AvroDataPreprocessingHelper(inputPaths, outputPath);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "DAILY"));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTableName").setIngestionConfig(ingestionConfig)
            .build();
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setTimeColumnName("time_day");
    segmentsValidationAndRetentionConfig.setTimeType("MILLISECONDS");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);

    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("time_day", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    dataPreprocessingHelper.registerConfigs(tableConfig, schema, "column2", 1, "Murmur", "0", "column4",
        FieldSpec.DataType.INT, "0", 0, 0);

    Job job = dataPreprocessingHelper.setUpJob();
    Configuration conf = job.getConfiguration();
    assertNotNull(job);
    assertNull(conf.get(InternalConfigConstants.SEGMENT_TIME_SDF_PATTERN));

    // Validate partitioning configs.
    assertEquals(conf.get(InternalConfigConstants.PARTITION_COLUMN_CONFIG), "column2");
    assertEquals(conf.get(InternalConfigConstants.PARTITION_FUNCTION_CONFIG), "Murmur");
    assertEquals(conf.get(InternalConfigConstants.NUM_PARTITIONS_CONFIG), "1");
    assertEquals(conf.get(InternalConfigConstants.PARTITION_COLUMN_DEFAULT_NULL_VALUE), "0");

    // Validate sorting configs.
    assertEquals(conf.get(InternalConfigConstants.SORTING_COLUMN_CONFIG), "column4");
    assertEquals(conf.get(InternalConfigConstants.SORTING_COLUMN_TYPE), "INT");
    assertEquals(conf.get(InternalConfigConstants.SORTING_COLUMN_DEFAULT_NULL_VALUE), "0");
  }
}

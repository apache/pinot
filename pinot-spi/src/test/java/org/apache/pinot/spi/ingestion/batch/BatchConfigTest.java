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
package org.apache.pinot.spi.ingestion.batch;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for BatchConfigs
 */
public class BatchConfigTest {

  @Test
  public void testBatchConfig() {
    Map<String, String> batchConfigMap = new HashMap<>();
    String tableName = "foo_REALTIME";
    String inputDir = "s3://foo/input";
    String outputDir = "s3://foo/output";
    String inputFsClass = "org.apache.S3FS";
    String outputFsClass = "org.apache.GcsGS";
    String region = "us-west";
    String username = "foo";
    String accessKey = "${ACCESS_KEY}";
    String secretKey = "${SECRET_KEY}";
    String inputFormat = "csv";
    String recordReaderClass = "org.foo.CSVRecordReader";
    String recordReaderConfigClass = "org.foo.CSVRecordReaderConfig";
    String separator = "|";
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, inputDir);
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, outputDir);
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, inputFsClass);
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_CLASS, outputFsClass);
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, inputFormat);
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CLASS, recordReaderClass);
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CONFIG_CLASS, recordReaderConfigClass);
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_PROP_PREFIX + ".region", region);
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_PROP_PREFIX + ".username", username);
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".region", region);
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".accessKey", accessKey);
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".secretKey", secretKey);
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".separator", separator);

    // config with all the right properties
    BatchConfig batchConfig = new BatchConfig(tableName, batchConfigMap);
    assertEquals(batchConfig.getInputDirURI(), inputDir);
    assertEquals(batchConfig.getOutputDirURI(), outputDir);
    assertEquals(batchConfig.getInputFsClassName(), inputFsClass);
    assertEquals(batchConfig.getOutputFsClassName(), outputFsClass);
    assertEquals(batchConfig.getInputFormat(), FileFormat.CSV);
    assertEquals(batchConfig.getRecordReaderClassName(), recordReaderClass);
    assertEquals(batchConfig.getRecordReaderConfigClassName(), recordReaderConfigClass);
    assertEquals(batchConfig.getInputFsProps().size(), 2);
    assertEquals(batchConfig.getInputFsProps().get(BatchConfigProperties.INPUT_FS_PROP_PREFIX + ".region"), region);
    assertEquals(batchConfig.getInputFsProps().get(BatchConfigProperties.INPUT_FS_PROP_PREFIX + ".username"), username);
    assertEquals(batchConfig.getOutputFsProps().size(), 3);
    assertEquals(batchConfig.getOutputFsProps().get(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".region"), region);
    assertEquals(batchConfig.getOutputFsProps().get(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".accessKey"), accessKey);
    assertEquals(batchConfig.getOutputFsProps().get(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX + ".secretKey"), secretKey);
    assertEquals(batchConfig.getRecordReaderProps().size(), 1);
    assertEquals(batchConfig.getRecordReaderProps().get(BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".separator"),
        separator);
    assertEquals(batchConfig.getTableNameWithType(), tableName);
  }
}

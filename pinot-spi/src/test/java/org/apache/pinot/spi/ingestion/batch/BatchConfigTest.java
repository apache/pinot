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
import org.testng.Assert;
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
    String batchType = "s3";
    String inputDir = "s3://foo/input";
    String outputDir = "s3://foo/output";
    String fsClass = "org.apache.S3FS";
    String region = "us-west";
    String username = "foo";
    String inputFormat = "csv";
    String recordReaderClass = "org.foo.CSVRecordReader";
    String recordReaderConfigClass = "org.foo.CSVRecordReaderConfig";
    String separator = "|";
    batchConfigMap.put(BatchConfigProperties.BATCH_TYPE, batchType);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.INPUT_DIR_URI), inputDir);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.OUTPUT_DIR_URI), outputDir);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_CLASS), fsClass);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.INPUT_FORMAT), inputFormat);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_CLASS),
            recordReaderClass);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_CONFIG_CLASS),
            recordReaderConfigClass);
    batchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_PROP_PREFIX) + ".region",
            region);
    batchConfigMap.put(
        BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_PROP_PREFIX) + ".username",
        username);
    batchConfigMap.put(
        BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_PROP_PREFIX)
            + ".separator", separator);

    // config with all the right properties
    BatchConfig batchConfig = new BatchConfig(tableName, batchConfigMap);
    assertEquals(batchConfig.getType(), batchType);
    assertEquals(batchConfig.getInputDirURI(), inputDir);
    assertEquals(batchConfig.getOutputDirURI(), outputDir);
    assertEquals(batchConfig.getFsClassName(), fsClass);
    assertEquals(batchConfig.getInputFormat(), FileFormat.CSV);
    assertEquals(batchConfig.getRecordReaderClassName(), recordReaderClass);
    assertEquals(batchConfig.getRecordReaderConfigClassName(), recordReaderConfigClass);
    assertEquals(batchConfig.getFsProps().size(), 2);
    assertEquals(batchConfig.getFsProps().get(
        BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_PROP_PREFIX) + ".region"),
        region);
    assertEquals(batchConfig.getFsProps().get(
        BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_PROP_PREFIX) + ".username"),
        username);
    assertEquals(batchConfig.getRecordReaderProps().size(), 1);
    assertEquals(batchConfig.getRecordReaderProps().get(
        BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_PROP_PREFIX)
            + ".separator"), separator);
    assertEquals(batchConfig.getTableNameWithType(), tableName);

    // Missing props
    Map<String, String> testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap.remove(BatchConfigProperties.BATCH_TYPE);
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'batchType");
    } catch (IllegalStateException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.INPUT_DIR_URI));
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'inputDirURI");
    } catch (IllegalStateException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.OUTPUT_DIR_URI));
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'outputDirURI");
    } catch (IllegalStateException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.INPUT_FORMAT));
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'inputFormat");
    } catch (IllegalStateException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .put(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.INPUT_FORMAT), "moo");
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for incorrect 'inputFormat");
    } catch (IllegalArgumentException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_CLASS));
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'recordReaderClassName");
    } catch (IllegalStateException e) {
      // expected
    }

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.RECORD_READER_CONFIG_CLASS));
    new BatchConfig(tableName, testBatchConfigMap);

    testBatchConfigMap = new HashMap<>(batchConfigMap);
    testBatchConfigMap
        .remove(BatchConfigProperties.constructStreamProperty(batchType, BatchConfigProperties.FS_CLASS));
    try {
      new BatchConfig(tableName, testBatchConfigMap);
      Assert.fail("Should fail for missing 'fsClassName");
    } catch (IllegalStateException e) {
      // expected
    }
  }
}

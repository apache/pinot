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
package org.apache.pinot.plugin.minion.tasks.segmentgenerationandpush;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.minion.event.DefaultMinionEventObserver;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Tests for [SegmentGenerationAndPushTaskGenerator]
public class SegmentGenerationAndPushTaskGeneratorTest {

  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final URI INPUT_FILE_URI = URI.create("file:///tmp/input/data.csv");
  private static final String APPEND_UUID_CONFIG_KEY =
      BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + "."
          + BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME;

  private static final String APPEND_UUID_TO_SEGMENT_NAME_KEY = SegmentGenerationTaskRunner.APPEND_UUID_TO_SEGMENT_NAME;

  private SegmentGenerationAndPushTaskGenerator _generator;

  @BeforeMethod
  public void setUp() {
    ClusterInfoAccessor clusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(clusterInfoAccessor.getVipUrlForLeadController(TABLE_NAME)).thenReturn("http://localhost:9000");
    _generator = new SegmentGenerationAndPushTaskGenerator();
    _generator.init(clusterInfoAccessor);
  }

  @Test
  public void testAppendUuidDefaultsToTrueWhenUnset()
      throws Exception {
    Map<String, String> taskConfig = invokeGetSingleFileGenerationTaskConfig(batchConfigMap(Map.of()));
    assertEquals(taskConfig.get(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME), "true");
  }

  @Test
  public void testAppendUuidRespectsExplicitTopLevelFalse()
      throws Exception {
    Map<String, String> taskConfig = invokeGetSingleFileGenerationTaskConfig(
        batchConfigMap(Map.of(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "false")));
    assertEquals(taskConfig.get(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME), "false");
  }

  @Test
  public void testAppendUuidRespectsExplicitPrefixedFalse()
      throws Exception {
    Map<String, String> taskConfig =
        invokeGetSingleFileGenerationTaskConfig(batchConfigMap(Map.of(APPEND_UUID_CONFIG_KEY, "false")));
    assertEquals(taskConfig.get(APPEND_UUID_CONFIG_KEY), "false");
    // Mirrored to the top-level key so that executors which only read the top-level key behave the same
    assertEquals(taskConfig.get(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME), "false");
  }

  @Test
  public void testAppendUuidPrefixedTakesPrecedenceOverTopLevel()
      throws Exception {
    Map<String, String> taskConfig = invokeGetSingleFileGenerationTaskConfig(
        batchConfigMap(Map.of(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "true", APPEND_UUID_CONFIG_KEY,
            "false")));
    assertEquals(taskConfig.get(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME), "false");
    assertEquals(taskConfig.get(APPEND_UUID_CONFIG_KEY), "false");
  }

  @Test
  public void testGeneratedTaskConfigIsHonoredByExecutor()
      throws Exception {
    // Table config -> generator -> executor: the spec consumed by the segment generation runner must carry the
    // configured value
    assertAppendUuidInExecutorSpec(Map.of(APPEND_UUID_CONFIG_KEY, "false"), "false");
    assertAppendUuidInExecutorSpec(Map.of(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "false"), "false");
    assertAppendUuidInExecutorSpec(Map.of(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "true",
        APPEND_UUID_CONFIG_KEY, "false"), "false");
    assertAppendUuidInExecutorSpec(Map.of(), "true");
  }

  private static Map<String, String> batchConfigMap(Map<String, String> extraConfigs) {
    Map<String, String> batchConfigMap = new HashMap<>(extraConfigs);
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, "file:///tmp/input");
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE, "inputFile");
    return batchConfigMap;
  }

  private void assertAppendUuidInExecutorSpec(Map<String, String> extraBatchConfigs, String expected)
      throws Exception {
    // Add the fields the executor normally receives from the controller
    Map<String, String> taskConfig =
        new HashMap<>(invokeGetSingleFileGenerationTaskConfig(batchConfigMap(extraBatchConfigs)));
    ClassLoader classLoader = getClass().getClassLoader();
    URL resourcesLoc = classLoader.getResource(".");
    assertNotNull(resourcesLoc);
    URL tableConfigUrl = classLoader.getResource("dummyTable.json");
    assertNotNull(tableConfigUrl);
    taskConfig.put(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, resourcesLoc + "dummyTable.json");
    taskConfig.put(BatchConfigProperties.RECORD_READER_CLASS, "AReaderClass");
    taskConfig.put(BatchConfigProperties.RECORD_READER_CONFIG_CLASS, "AReaderConfigClass");
    taskConfig.put(BatchConfigProperties.SCHEMA, new Schema.SchemaBuilder().build().toSingleLineJsonString());
    taskConfig.put(BatchConfigProperties.TABLE_CONFIGS,
        FileUtils.readFileToString(new File(tableConfigUrl.getFile()), StandardCharsets.UTF_8));

    SegmentGenerationAndPushTaskExecutor executor = new SegmentGenerationAndPushTaskExecutor();
    FieldUtils.writeField(executor, "_eventObserver", new DefaultMinionEventObserver(), true);
    SegmentGenerationTaskSpec spec = executor.generateTaskSpec(taskConfig, Paths.get(resourcesLoc.toURI()).toFile());
    assertEquals(spec.getSegmentNameGeneratorSpec().getConfigs().get(APPEND_UUID_TO_SEGMENT_NAME_KEY), expected);
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> invokeGetSingleFileGenerationTaskConfig(Map<String, String> batchConfigMap)
      throws Exception {
    return (Map<String, String>) MethodUtils.invokeMethod(_generator, true, "getSingleFileGenerationTaskConfig",
        TABLE_NAME, 0, batchConfigMap, INPUT_FILE_URI, null);
  }
}

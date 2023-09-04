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

import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.event.DefaultMinionEventObserver;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for {@link SegmentGenerationAndPushTaskGeneratorTest}
 */
public class SegmentGenerationAndPushTaskGeneratorTest extends ControllerTest {
  SegmentGenerationAndPushTaskGenerator _generator;

  @BeforeClass
  public void setup()
      throws Exception {
    int zkPort = 2171;
    startZk(zkPort);
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ZK_STR, "localhost:" + zkPort);
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, SegmentGenerationAndPushTaskGeneratorTest.class.getSimpleName());
    properties.put(ControllerConf.CONTROLLER_PORT, 28998);
    startController(properties);

    ClusterInfoAccessor clusterInfoAccessor = _controllerStarter.getTaskManager().getClusterInfoAccessor();
    _generator = new SegmentGenerationAndPushTaskGenerator();
    _generator.init(clusterInfoAccessor);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  @Test
  public void testRealCluster()
      throws Exception {
    // Default is 1
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 1);

    // Set config to 5
    String request = JsonUtils.objectToString(Collections
        .singletonMap(MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE, "5"));
    ControllerTest.sendPostRequest(_controllerRequestURLBuilder.forClusterConfigs(), request);
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 5);

    // Set config to invalid and should still get 1
    request = JsonUtils.objectToString(Collections
        .singletonMap(MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE,
            "abcd"));
    ControllerTest.sendPostRequest(_controllerRequestURLBuilder.forClusterConfigs(), request);
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 1);
  }

  @Test
  public void testGenerateTaskSpec() throws Exception {
    URL resourcesLoc = SegmentGenerationAndPushTaskGeneratorTest.class.getClassLoader().getResource(".");
    SegmentGenerationAndPushTaskExecutor executor = new SegmentGenerationAndPushTaskExecutor();
    Schema schema = new Schema.SchemaBuilder().build();
    FieldUtils.writeField(executor, "_eventObserver", new DefaultMinionEventObserver(), true);
    Map<String, String> configMap = Stream.of(new String[][] {
      {BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, resourcesLoc.toString() + "dummyTable.json"},
      {BatchConfigProperties.INPUT_FORMAT, ""},
      {BatchConfigProperties.RECORD_READER_CLASS, "AReaderClass"},
      {BatchConfigProperties.RECORD_READER_CONFIG_CLASS, "AReaderConfigClass"},
      {BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".prop1", "value1"},
      {BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".prop.2", "value2"},
      {BatchConfigProperties.AUTH_TOKEN, "not_used"},
      {BatchConfigProperties.TABLE_NAME, "not_used"},
      {BatchConfigProperties.SCHEMA, schema.toSingleLineJsonString()},
      {BatchConfigProperties.SCHEMA_URI, "not_used"},
      {BatchConfigProperties.TABLE_CONFIGS,
          new String(SegmentGenerationAndPushTaskGeneratorTest.class.getClassLoader()
              .getResourceAsStream("dummyTable.json").readAllBytes())},
      {BatchConfigProperties.TABLE_CONFIGS_URI, "not_used"},
      {BatchConfigProperties.SEQUENCE_ID, "42"},
      {BatchConfigProperties.FAIL_ON_EMPTY_SEGMENT, "true"},
      {BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE, "inputtext"},
      {BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + ".prop.seg.1", "valseg1"},
      {BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + ".propseg2", "valseg2"},
      {BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "true"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    SegmentGenerationTaskSpec spec = executor.generateTaskSpec(configMap, Paths.get(resourcesLoc.toURI()).toFile());
    Assert.assertEquals(spec.getSequenceId(), 42);
    Assert.assertEquals("file:" + spec.getInputFilePath(), resourcesLoc.toString() + "input/dummyTable.json");
    Assert.assertEquals(spec.getRecordReaderSpec().getClassName(), "AReaderClass");
    Assert.assertEquals(spec.getRecordReaderSpec().getConfigClassName(), "AReaderConfigClass");
    Assert.assertEqualsDeep(spec.getRecordReaderSpec().getConfigs(), Stream.of(new String[][] {
        {"prop1", "value1"},
        {"prop.2", "value2"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1])));
    Assert.assertEquals(spec.isFailOnEmptySegment(), true);
    Assert.assertEquals(spec.getSegmentNameGeneratorSpec().getType(), "inputtext");
    Assert.assertEqualsDeep(spec.getSegmentNameGeneratorSpec().getConfigs(), Stream.of(new String[][] {
        {"prop.seg.1", "valseg1"},
        {"propseg2", "valseg2"},
        {SegmentGenerationTaskRunner.APPEND_UUID_TO_SEGMENT_NAME, "true"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1])));
  }
}

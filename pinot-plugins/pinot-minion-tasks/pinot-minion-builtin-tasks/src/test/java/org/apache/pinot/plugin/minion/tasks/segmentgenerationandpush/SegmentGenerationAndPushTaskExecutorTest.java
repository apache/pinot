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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.minion.event.DefaultMinionEventObserver;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link SegmentGenerationAndPushTaskExecutor}
 */
public class SegmentGenerationAndPushTaskExecutorTest {

  @Test
  public void testGenerateTaskSpec()
      throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resourcesLoc = classLoader.getResource(".");
    assertNotNull(resourcesLoc);
    URL tableConfigUrl = classLoader.getResource("dummyTable.json");
    assertNotNull(tableConfigUrl);
    String tableConfig = FileUtils.readFileToString(new File(tableConfigUrl.getFile()), StandardCharsets.UTF_8);
    SegmentGenerationAndPushTaskExecutor executor = new SegmentGenerationAndPushTaskExecutor();
    Schema schema = new Schema.SchemaBuilder().build();
    FieldUtils.writeField(executor, "_eventObserver", new DefaultMinionEventObserver(), true);
    Map<String, String> configMap = new HashMap<>() {{
      put(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, resourcesLoc + "dummyTable.json");
      put(BatchConfigProperties.INPUT_FORMAT, "");
      put(BatchConfigProperties.RECORD_READER_CLASS, "AReaderClass");
      put(BatchConfigProperties.RECORD_READER_CONFIG_CLASS, "AReaderConfigClass");
      put(BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".prop1", "value1");
      put(BatchConfigProperties.RECORD_READER_PROP_PREFIX + ".prop.2", "value2");
      put(BatchConfigProperties.AUTH_TOKEN, "not_used");
      put(BatchConfigProperties.TABLE_NAME, "not_used");
      put(BatchConfigProperties.SCHEMA, schema.toSingleLineJsonString());
      put(BatchConfigProperties.SCHEMA_URI, "not_used");
      put(BatchConfigProperties.TABLE_CONFIGS, tableConfig);
      put(BatchConfigProperties.TABLE_CONFIGS_URI, "not_used");
      put(BatchConfigProperties.SEQUENCE_ID, "42");
      put(BatchConfigProperties.FAIL_ON_EMPTY_SEGMENT, "true");
      put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE, "inputtext");
      put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + ".prop.seg.1", "valseg1");
      put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX + ".propseg2", "valseg2");
      put(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME, "true");
    }};
    SegmentGenerationTaskSpec spec = executor.generateTaskSpec(configMap, Paths.get(resourcesLoc.toURI()).toFile());
    assertEquals(spec.getSequenceId(), 42);
    assertEquals("file:" + spec.getInputFilePath(), resourcesLoc + "input/dummyTable.json");
    assertEquals(spec.getRecordReaderSpec().getClassName(), "AReaderClass");
    assertEquals(spec.getRecordReaderSpec().getConfigClassName(), "AReaderConfigClass");
    assertEqualsDeep(spec.getRecordReaderSpec().getConfigs(), Map.of("prop1", "value1", "prop.2", "value2"));
    assertTrue(spec.isFailOnEmptySegment());
    assertEquals(spec.getSegmentNameGeneratorSpec().getType(), "inputtext");
    assertEqualsDeep(spec.getSegmentNameGeneratorSpec().getConfigs(),
        Map.of("prop.seg.1", "valseg1", "propseg2", "valseg2", SegmentGenerationTaskRunner.APPEND_UUID_TO_SEGMENT_NAME,
            "true"));
  }
}

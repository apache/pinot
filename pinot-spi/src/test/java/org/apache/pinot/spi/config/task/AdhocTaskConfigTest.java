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
package org.apache.pinot.spi.config.task;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class AdhocTaskConfigTest {

  @Test
  public void testDeserializeFromJson()
      throws IOException {
    AdhocTaskConfig adhocTaskConfig = new AdhocTaskConfig("SegmentGenerationAndPushTask", "myTable", "myTask-0",
        ImmutableMap.of("inputDirURI", "s3://my-bucket/my-file.json"));
    adhocTaskConfig = JsonUtils.stringToObject(JsonUtils.objectToString(adhocTaskConfig), AdhocTaskConfig.class);
    assertEquals(adhocTaskConfig.getTaskType(), "SegmentGenerationAndPushTask");
    assertEquals(adhocTaskConfig.getTableName(), "myTable");
    assertEquals(adhocTaskConfig.getTaskName(), "myTask-0");
    assertEquals(adhocTaskConfig.getTaskConfigs().size(), 1);
    assertEquals(adhocTaskConfig.getTaskConfigs().get("inputDirURI"), "s3://my-bucket/my-file.json");
  }

  @Test
  public void testInvalidArgumentsForAdhocTaskConfig() {
    // Test 1 : pass invalid taskType(null) to AdhocTaskConfig.
    assertThrows(IllegalArgumentException.class, () -> new AdhocTaskConfig(null, "TestTable", "TestTaskName",
        ImmutableMap.of("inputDirURI", "s3://my-bucket/my-file.json")));

    // Test 2 : pass invalid tableName(null) to AdhocTaskConfig.
    assertThrows(IllegalArgumentException.class,
        () -> new AdhocTaskConfig("SegmentGenerationAndPushTask", null, "TestTaskName",
            ImmutableMap.of("inputDirURI", "s3://my-bucket/my-file.json")));

    //Test 3 : pass invalid taskName(String with path separator '/') to AdhocTaskConfig.
    assertThrows(IllegalArgumentException.class,
        () -> new AdhocTaskConfig("SegmentGenerationAndPushTask", "TestTable", "Invalid/TestTaskName",
            ImmutableMap.of("inputDirURI", "s3://my-bucket/my-file.json")));
  }
}

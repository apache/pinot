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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class SourceFieldConfigTest {

  @Test
  public void deserializesAllFields()
      throws JsonProcessingException {
    SourceFieldConfig config = JsonUtils.stringToObject(
        "{\"name\": \"ts\", \"dataType\": \"LONG\", \"preComplexTypeTransform\": true}", SourceFieldConfig.class);
    assertEquals(config.getName(), "ts");
    assertEquals(config.getDataType(), PinotDataType.LONG);
    assertTrue(config.isPreComplexTypeTransform());
  }

  @Test
  public void preComplexTypeTransformDefaultsToFalse()
      throws JsonProcessingException {
    SourceFieldConfig config =
        JsonUtils.stringToObject("{\"name\": \"tags\", \"dataType\": \"STRING_ARRAY\"}", SourceFieldConfig.class);
    assertEquals(config.getDataType(), PinotDataType.STRING_ARRAY);
    assertFalse(config.isPreComplexTypeTransform());
  }

  @Test
  public void roundTripsThroughJson()
      throws JsonProcessingException {
    SourceFieldConfig config = new SourceFieldConfig("ts", PinotDataType.INT, false);
    assertEquals(JsonUtils.stringToObject(config.toJsonString(), SourceFieldConfig.class), config);
  }

  @Test
  public void rejectsMissingName() {
    assertThrows(IllegalArgumentException.class, () -> new SourceFieldConfig(null, PinotDataType.INT, false));
    assertThrows(IllegalArgumentException.class, () -> new SourceFieldConfig("", PinotDataType.INT, false));
    assertThrows(IllegalArgumentException.class, () -> new SourceFieldConfig("   ", PinotDataType.INT, false));
  }

  @Test
  public void rejectsMissingDataType() {
    assertThrows(IllegalArgumentException.class, () -> new SourceFieldConfig("ts", null, false));
  }
}

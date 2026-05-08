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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.vector.VectorIndexType;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class VectorIndexTest {
  @Test
  public void testEmptyPropertiesSurviveRoundTripForRuntimeDefaults()
      throws Exception {
    VectorIndexConfig original = new VectorIndexConfig(Boolean.FALSE, "HNSW", 128, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of());
    String serialized = JsonUtils.objectToString(original);
    VectorIndexConfig roundTripped = JsonUtils.stringToObject(serialized, VectorIndexConfig.class);

    assertNotNull(roundTripped.getProperties());
    assertTrue(roundTripped.getProperties().isEmpty());
    // Regression guard: this used to NPE when properties became null after round-trip.
    assertNotNull(VectorIndexUtils.getIndexWriterConfig(roundTripped));
  }

  public static class ConfTest extends AbstractSerdeIndexContract {

    @Test
    public void testConvertToUpdatedFormat()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "  \"name\": \"studentID\",\n"
          + "  \"encodingType\": \"DICTIONARY\",\n"
          + "  \"indexTypes\": [\n"
          + "    \"VECTOR\"\n"
          + "  ],\n"
          + "  \"properties\": {\n"
          + "    \"vectorIndexType\": \"HNSW\",\n"
          + "    \"vectorDimension\": 1536,\n"
          + "    \"vectorDistanceFunction\": \"COSINE\",\n"
          + "    \"version\": 1\n"
          + "  },\n"
          + "  \"tierOverwrites\": null\n"
          + "}");
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("studentID")).collect(Collectors.toList()).get(0);
      JsonNode indexConfig = fieldConfig.getIndexes().get(VectorIndexType.INDEX_DISPLAY_NAME);
      assertNotNull(indexConfig);
      // Slim serialization: disabled=false (default) is omitted from the JSON entirely.
      assertNull(indexConfig.get("disabled"));
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
      assertNull(fieldConfig.getProperties());
      Assert.assertEquals(indexConfig.toString(),
          "{\"vectorIndexType\":\"HNSW\",\"vectorDimension\":1536,"
              + "\"version\":1,\"vectorDistanceFunction\":\"COSINE\",\"properties\":{\"vectorIndexType\":"
              + "\"HNSW\",\"vectorDimension\":\"1536\",\"vectorDistanceFunction\":\"COSINE\",\"version\":\"1\"}}");
    }
  }
}

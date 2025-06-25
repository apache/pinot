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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.index.creator.VectorIndexConfig.VectorDistanceFunction.COSINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class VectorConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    VectorIndexConfig config = JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getVectorIndexType(), "Unexpected vectorIndexType");
    assertNull(config.getVectorDistanceFunction(), "Unexpected vectorDistanceFunction");
    assertNull(config.getProperties(), "unexpected properties");
    assertEquals(config.getVectorDimension(), 0);
    assertEquals(config.getVersion(), 0);
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    VectorIndexConfig config = JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getVectorIndexType(), "Unexpected vectorIndexType");
    assertNull(config.getVectorDistanceFunction(), "Unexpected vectorDistanceFunction");
    assertNull(config.getProperties(), "unexpected properties");
    assertEquals(config.getVectorDimension(), 0);
    assertEquals(config.getVersion(), 0);
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    VectorIndexConfig config = JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getVectorIndexType(), "Unexpected vectorIndexType");
    assertNull(config.getVectorDistanceFunction(), "Unexpected vectorDistanceFunction");
    assertNull(config.getProperties(), "unexpected properties");
    assertEquals(config.getVectorDimension(), 0);
    assertEquals(config.getVersion(), 0);
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    VectorIndexConfig config = JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getVectorIndexType(), "Unexpected vectorIndexType");
    assertNull(config.getVectorDistanceFunction(), "Unexpected vectorDistanceFunction");
    assertNull(config.getProperties(), "unexpected properties");
    assertEquals(config.getVectorDimension(), 0);
    assertEquals(config.getVersion(), 0);
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "  \"version\": 1,\n"
        + "  \"properties\": {\n"
        + "    \"vectorIndexType\": \"HNSW\",\n"
        + "    \"vectorDimension\": \"1536\",\n"
        + "    \"vectorDistanceFunction\": \"COSINE\",\n"
        + "    \"version\": \"1\"\n"
        + "  },\n"
        + "  \"vectorDistanceFunction\": \"COSINE\",\n"
        + "  \"vectorIndexType\": \"HNSW\",\n"
        + "  \"vectorDimension\": 1536,\n"
        + "  \"disabled\": false\n"
        + "}";
    VectorIndexConfig config = JsonUtils.stringToObject(confStr, VectorIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVectorDimension(), 1536);
    assertEquals(config.getVersion(), 1);
    assertEquals(config.getVectorIndexType(), "HNSW");
    assertEquals(config.getVectorDistanceFunction(), COSINE);

    Map<String, String> properties = config.getProperties();
    assertEquals(properties.getOrDefault("vectorDimension", "0"), "1536");
    assertEquals(properties.getOrDefault("version", "0"), "1");
    assertEquals(properties.getOrDefault("vectorIndexType", ""), "HNSW");
    assertEquals(properties.getOrDefault("vectorDistanceFunction", ""), COSINE.toString());
  }

  @Test
  public void serde()
      throws JsonProcessingException {
    VectorIndexConfig initialConf = new VectorIndexConfig(false);

    String confAsJson = JsonUtils.objectToString(initialConf);
    Assert.assertEquals(confAsJson, "{\"disabled\":false,\"vectorDimension\":0,\"version\":0}");

    VectorIndexConfig readConf = JsonUtils.stringToObject(confAsJson, VectorIndexConfig.class);
    Assert.assertEquals(readConf, initialConf, "Unexpected configuration after serialization and deserialization");
  }

  @Test
  public void deserializeShort()
      throws JsonProcessingException {
    VectorIndexConfig initialConf = new VectorIndexConfig(false);

    String serialized = "{\"disabled\":false,\"vectorDimension\":0,\"version\":0}";
    VectorIndexConfig vectorIndexConfig = JsonUtils.stringToObject(serialized, VectorIndexConfig.class);

    Assert.assertEquals(vectorIndexConfig, initialConf, "Unexpected configuration after reading " + serialized);
  }
}

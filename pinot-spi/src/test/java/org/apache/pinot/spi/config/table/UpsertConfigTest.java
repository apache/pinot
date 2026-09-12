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
package org.apache.pinot.spi.config.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class UpsertConfigTest {

  @Test
  public void testUpsertConfig() {
    UpsertConfig upsertConfig1 = new UpsertConfig(UpsertConfig.Mode.FULL);
    assertEquals(upsertConfig1.getMode(), UpsertConfig.Mode.FULL);

    upsertConfig1.setComparisonColumn("comparison");
    assertEquals(upsertConfig1.getComparisonColumns(), List.of("comparison"));

    upsertConfig1.setHashFunction(HashFunction.MURMUR3);
    assertEquals(upsertConfig1.getHashFunction(), HashFunction.MURMUR3);

    UpsertConfig upsertConfig2 = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("myCol", UpsertConfig.Strategy.INCREMENT);
    upsertConfig2.setPartialUpsertStrategies(partialUpsertStrategies);
    upsertConfig2.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    assertEquals(upsertConfig2.getPartialUpsertStrategies(), partialUpsertStrategies);

    Map<String, String> partialUpsertMergerConfigs = Map.of("jsonColumns", "profile,attributes");
    upsertConfig2.setPartialUpsertMergerConfigs(partialUpsertMergerConfigs);
    assertEquals(upsertConfig2.getPartialUpsertMergerConfigs(), partialUpsertMergerConfigs);
  }

  @Test
  public void testUpsertConfigForDefaults() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    assertEquals(upsertConfig.getHashFunction(), HashFunction.NONE);
    assertEquals(upsertConfig.getDefaultPartialUpsertStrategy(), UpsertConfig.Strategy.OVERWRITE);
  }

  @Test
  public void testPartialUpsertMergerConfigsJsonRoundTrip()
      throws Exception {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setPartialUpsertMergerClass("example.StructuredMerger");
    upsertConfig.setPartialUpsertMergerConfigs(Map.of("jsonColumns", "profile", "maxDepth", "32"));

    UpsertConfig deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(upsertConfig), UpsertConfig.class);

    assertEquals(deserialized.getPartialUpsertMergerClass(), "example.StructuredMerger");
    assertEquals(deserialized.getPartialUpsertMergerConfigs(), Map.of("jsonColumns", "profile", "maxDepth", "32"));

    UpsertConfig oldConfig = JsonUtils.stringToObject("{\"mode\":\"PARTIAL\"}", UpsertConfig.class);
    assertNull(oldConfig.getPartialUpsertMergerConfigs());
  }
}

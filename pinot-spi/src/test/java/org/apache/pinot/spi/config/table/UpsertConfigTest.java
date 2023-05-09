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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class UpsertConfigTest {

  @Test
  public void testUpsertConfig() {
    UpsertConfig upsertConfig1 = new UpsertConfig(UpsertConfig.Mode.FULL);
    assertEquals(upsertConfig1.getMode(), UpsertConfig.Mode.FULL);

    upsertConfig1.setComparisonColumn("comparison");
    assertEquals(upsertConfig1.getComparisonColumns(), Collections.singletonList("comparison"));

    upsertConfig1.setHashFunction(HashFunction.MURMUR3);
    assertEquals(upsertConfig1.getHashFunction(), HashFunction.MURMUR3);

    UpsertConfig upsertConfig2 = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    Map<String, UpsertConfig.Strategy> partialUpsertStratgies = new HashMap<>();
    partialUpsertStratgies.put("myCol", UpsertConfig.Strategy.INCREMENT);
    upsertConfig2.setPartialUpsertStrategies(partialUpsertStratgies);
    upsertConfig2.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    assertEquals(upsertConfig2.getPartialUpsertStrategies(), partialUpsertStratgies);
  }

  @Test
  public void testUpsertConfigForDefaults() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    assertEquals(upsertConfig.getHashFunction(), HashFunction.NONE);
    assertEquals(upsertConfig.getDefaultPartialUpsertStrategy(), UpsertConfig.Strategy.OVERWRITE);
  }
}

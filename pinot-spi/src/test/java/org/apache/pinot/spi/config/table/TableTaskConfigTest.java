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
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableTaskConfigTest {

  @Test
  public void testSingleArgConstructorDefaultsConcurrentSchedulingToNull() {
    TableTaskConfig config = new TableTaskConfig(Map.of("TestTask", Collections.emptyMap()));
    Assert.assertNull(config.getConcurrentSchedulingEnabled(),
        "concurrentSchedulingEnabled should default to null (inherit cluster setting)");
  }

  @Test
  public void testTwoArgConstructorPreservesExplicitValue() {
    TableTaskConfig trueConfig =
        new TableTaskConfig(Map.of("TestTask", Collections.emptyMap()), Boolean.TRUE);
    Assert.assertEquals(trueConfig.getConcurrentSchedulingEnabled(), Boolean.TRUE);

    TableTaskConfig falseConfig =
        new TableTaskConfig(Map.of("TestTask", Collections.emptyMap()), Boolean.FALSE);
    Assert.assertEquals(falseConfig.getConcurrentSchedulingEnabled(), Boolean.FALSE);

    TableTaskConfig nullConfig =
        new TableTaskConfig(Map.of("TestTask", Collections.emptyMap()), null);
    Assert.assertNull(nullConfig.getConcurrentSchedulingEnabled());
  }

  @Test
  public void testJsonRoundTripWithConcurrentSchedulingEnabled()
      throws Exception {
    TableTaskConfig original =
        new TableTaskConfig(Map.of("TestTask", Map.of("schedule", "0 * * ? * * *")), Boolean.TRUE);
    String json = JsonUtils.objectToString(original);
    TableTaskConfig parsed = JsonUtils.stringToObject(json, TableTaskConfig.class);
    Assert.assertEquals(parsed.getConcurrentSchedulingEnabled(), Boolean.TRUE);
    Assert.assertEquals(parsed.getTaskTypeConfigsMap(), original.getTaskTypeConfigsMap());
  }

  @Test
  public void testJsonRoundTripWithConcurrentSchedulingDisabled()
      throws Exception {
    TableTaskConfig original =
        new TableTaskConfig(Map.of("TestTask", Map.of("schedule", "0 * * ? * * *")), Boolean.FALSE);
    String json = JsonUtils.objectToString(original);
    TableTaskConfig parsed = JsonUtils.stringToObject(json, TableTaskConfig.class);
    Assert.assertEquals(parsed.getConcurrentSchedulingEnabled(), Boolean.FALSE);
  }

  @Test
  public void testJsonRoundTripDefaultsToNullWhenFieldOmitted()
      throws Exception {
    String json = "{\"taskTypeConfigsMap\":{\"TestTask\":{\"schedule\":\"0 * * ? * * *\"}}}";
    TableTaskConfig parsed = JsonUtils.stringToObject(json, TableTaskConfig.class);
    Assert.assertNull(parsed.getConcurrentSchedulingEnabled(),
        "Missing concurrentSchedulingEnabled field should parse as null (inherit cluster setting)");
    Assert.assertTrue(parsed.isTaskTypeEnabled("TestTask"));
  }

  @Test
  public void testJsonParseExplicitNull()
      throws Exception {
    String json = "{\"taskTypeConfigsMap\":{\"TestTask\":{}},\"concurrentSchedulingEnabled\":null}";
    TableTaskConfig parsed = JsonUtils.stringToObject(json, TableTaskConfig.class);
    Assert.assertNull(parsed.getConcurrentSchedulingEnabled());
  }
}

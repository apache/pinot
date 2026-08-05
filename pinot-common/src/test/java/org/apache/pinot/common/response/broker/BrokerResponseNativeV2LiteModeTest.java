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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BrokerResponseNativeV2LiteModeTest {

  @Test
  public void testLiteModeLeafStageLimitReachedMarksPartialResult() {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    assertFalse(brokerResponse.isMseLiteLeafStageLimitReached());
    assertFalse(brokerResponse.isPartialResult());

    brokerResponse.mergeMseLiteLeafStageLimitReached(true);

    assertTrue(brokerResponse.isMseLiteLeafStageLimitReached());
    assertTrue(brokerResponse.isPartialResult());
  }

  @Test
  public void testLiteModeLeafStageLimitReachedViaStatMap() {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.addBrokerStats(new StatMap<>(BrokerResponseNativeV2.StatKey.class)
        .merge(BrokerResponseNativeV2.StatKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED, true));

    assertTrue(brokerResponse.isMseLiteLeafStageLimitReached());
    assertTrue(brokerResponse.isPartialResult());
  }

  @Test
  public void testLiteModeFieldsInJsonSerialization()
      throws Exception {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.mergeMseLiteLeafStageLimitReached(true);
    brokerResponse.setMseLiteLeafStageEffectiveLimit(100);
    brokerResponse.setMseLiteFanOutAdjustedLimitApplied(true);

    JsonNode json = JsonUtils.objectToJsonNode(brokerResponse);

    assertTrue(json.path("mseLiteLeafStageLimitReached").asBoolean(false));
    assertEquals(json.path("mseLiteLeafStageEffectiveLimit").asInt(), 100);
    assertTrue(json.path("mseLiteFanOutAdjustedLimitApplied").asBoolean(false));
    assertTrue(json.path("partialResult").asBoolean(false));
  }

  @Test
  public void testLiteModeFieldsNotSerializedWhenDefault()
      throws Exception {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();

    JsonNode json = JsonUtils.objectToJsonNode(brokerResponse);

    assertFalse(json.has("mseLiteLeafStageEffectiveLimit"));
    assertFalse(json.has("mseLiteFanOutAdjustedLimitApplied"));
  }

  @Test
  public void testPartialResultNotSetWhenLimitNotReached() {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.setMseLiteLeafStageEffectiveLimit(100);

    assertFalse(brokerResponse.isMseLiteLeafStageLimitReached());
    assertFalse(brokerResponse.isPartialResult());
  }
}

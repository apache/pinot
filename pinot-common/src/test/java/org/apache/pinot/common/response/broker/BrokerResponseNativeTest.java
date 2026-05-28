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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BrokerResponseNativeTest {
  @Test
  public void testEmptyResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.EMPTY_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getNumDocsScanned(), expected.getNumDocsScanned());
    Assert.assertEquals(actual.getTimeUsedMs(), expected.getTimeUsedMs());
    Assert.assertEquals(actual.getResultTable(), expected.getResultTable());
  }

  @Test
  public void testNullResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getExceptions().get(0).getErrorCode(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getId());
    Assert.assertEquals(actual.getExceptions().get(0).getMessage(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getDefaultMessage());
  }

  @Test
  public void testMultipleExceptionsResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String errorMsgStr = "Some random string!";
    expected.addException(new QueryProcessingException(400, errorMsgStr));
    String brokerString = expected.toJsonString();
    BrokerResponseNative newBrokerResponse = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getId());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(),
        QueryErrorCode.BROKER_RESOURCE_MISSING.getDefaultMessage());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getErrorCode(), 400);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getMessage(), errorMsgStr);
  }

  @Test
  public void testMaterializedViewResponseFields()
      throws IOException {
    BrokerResponseNative expected = new BrokerResponseNative();
    expected.setMaterializedViewQueried("orders_by_day_OFFLINE");

    String brokerString = expected.toJsonString();
    Assert.assertTrue(brokerString.contains("\"materializedViewQueried\""));

    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getMaterializedViewQueried(), expected.getMaterializedViewQueried());
  }

  @Test
  public void testMaterializedViewQueriedAbsentWhenNull()
      throws IOException {
    BrokerResponseNative response = new BrokerResponseNative();
    String json = response.toJsonString();
    Assert.assertFalse(json.contains("materializedViewQueried"),
        "Null materializedViewQueried should be suppressed by @JsonInclude(NON_NULL)");
  }

  @Test
  public void testServerStatsRoundTrip()
      throws IOException {
    BrokerResponseNative expected = new BrokerResponseNative();
    String stats =
        "Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs,RequestSentDelayMs;"
            + "pinot-server-0_O=0,1,7571,0,0;pinot-server-1_O=0,1,7574,0,0";
    expected.setServerStats(stats);

    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(expected.toJsonString());
    Assert.assertEquals(actual.getServerStats(), stats);
  }

  @Test
  public void testServerStatsAbsentWhenNull()
      throws IOException {
    BrokerResponseNative response = new BrokerResponseNative();
    JsonNode tree = new ObjectMapper().readTree(response.toJsonString());
    Assert.assertFalse(tree.has("serverStats"));
  }

  @Test
  public void testServerStatsDefaultsToNull() {
    Assert.assertNull(new BrokerResponseNative().getServerStats());
  }
}

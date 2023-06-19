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
package org.apache.pinot.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BrokerResponseTest {
  private static final ObjectReader OBJECT_READER = JsonUtils.DEFAULT_READER;

  @Test
  public void parseResultWithRequestId()
      throws JsonProcessingException {
    String responseJson = "{\"requestId\":\"1\",\"traceInfo\":{},\"numDocsScanned\":36542,"
        + "\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"36542\"}],\"timeUsedMs\":30,"
        + "\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":115545}";
    BrokerResponse brokerResponse = BrokerResponse.fromJson(OBJECT_READER.readTree(responseJson));
    Assert.assertEquals("1", brokerResponse.getRequestId());
    Assert.assertTrue(!brokerResponse.hasExceptions());
  }

  @Test
  public void parseResultWithoutRequestId()
      throws JsonProcessingException {
    String responseJson = "{\"traceInfo\":{},\"numDocsScanned\":36542,"
        + "\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"36542\"}],\"timeUsedMs\":30,"
        + "\"segmentStatistics\":[],\"exceptions\":[],\"totalDocs\":115545}";
    BrokerResponse brokerResponse = BrokerResponse.fromJson(OBJECT_READER.readTree(responseJson));
    Assert.assertEquals("-1", brokerResponse.getRequestId());
    Assert.assertTrue(!brokerResponse.hasExceptions());
  }
}

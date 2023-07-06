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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import java.util.Properties;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiStageEngineJsonPathClusterIntegrationTest extends JsonPathClusterIntegrationTest {

  @Override
  protected Connection getPinotConnection() {
    Properties properties = new Properties();
    properties.put("queryOptions", "useMultistageEngine=true");
    if (_pinotConnection == null) {
      _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName());
    }
    return _pinotConnection;
  }

  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    ClusterIntegrationTestUtils.testQuery(pinotQuery, _brokerBaseApiUrl, getPinotConnection(), h2Query,
        getH2Connection(), null, ImmutableMap.of("queryOptions", "useMultistageEngine=true"));
  }

  /**
   * Queries the broker's sql query endpoint (/query/sql)
   */
  @Override
  protected JsonNode postQuery(String query)
      throws Exception {
    return postQuery(query, _brokerBaseApiUrl, null, ImmutableMap.of("queryOptions", "useMultistageEngine=true"));
  }

  @Test
  public void testComplexQueries2()
      throws Exception {
    //Group By Query
    String query = "Select" + " jsonExtractScalar(complexMapStr,'$.k1','STRING'),"
        + " sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT'))" + " from " + DEFAULT_TABLE_NAME
        + " group by jsonExtractScalar(complexMapStr,'$.k1','STRING')"
        + " order by sum(jsonExtractScalar(complexMapStr,'$.k4.met','INT')) DESC";
    JsonNode pinotResponse = postQuery(query);
    Assert.assertNotNull(pinotResponse.get("resultTable").get("rows"));
    ArrayNode rows = (ArrayNode) pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      String seqId = String.valueOf(NUM_TOTAL_DOCS - 1 - i);
      final JsonNode row = rows.get(i);
      Assert.assertEquals(row.get(0).asText(), "value-k1-" + seqId);
      Assert.assertEquals(row.get(1).asDouble(), Double.parseDouble(seqId));
    }
  }
}

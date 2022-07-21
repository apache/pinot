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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the various options set in the broker request
 */
public class BrokerRequestOptionsTest {
  // TODO: remove this legacy option size checker after 0.11 release cut.
  private static final int LEGACY_PQL_QUERY_OPTION_SIZE = 2;

  @Test
  public void testSetOptions() {
    long requestId = 1;
    String query = "select * from testTable";

    // None of the options
    ObjectNode jsonRequest = JsonUtils.newObjectNode();
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0 + LEGACY_PQL_QUERY_OPTION_SIZE);

    // TRACE
    // Has trace false
    jsonRequest.put(Request.TRACE, false);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0 + LEGACY_PQL_QUERY_OPTION_SIZE);

    // Has trace true
    jsonRequest.put(Request.TRACE, true);
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get(Request.TRACE), "true");

    // DEBUG_OPTIONS (debug options will also be included as query options)
    // Has debugOptions
    jsonRequest = JsonUtils.newObjectNode();
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertEquals(pinotQuery.getDebugOptionsSize(), 1);
    Assert.assertEquals(pinotQuery.getDebugOptions().get("debugOption1"), "foo");

    // Has multiple debugOptions
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo;debugOption2=bar");
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertEquals(pinotQuery.getDebugOptionsSize(), 2);
    Assert.assertEquals(pinotQuery.getDebugOptions().get("debugOption1"), "foo");
    Assert.assertEquals(pinotQuery.getDebugOptions().get("debugOption2"), "bar");

    // Invalid debug options
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1");
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    try {
      BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
      Assert.fail();
    } catch (Exception e) {
      // Expected
    }

    // QUERY_OPTIONS
    jsonRequest = JsonUtils.newObjectNode();
    // Has queryOptions in pinotQuery already
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("queryOption1", "foo");
    pinotQuery.setQueryOptions(queryOptions);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption1"), "foo");

    // Has queryOptions in query
    pinotQuery = CalciteSqlParser.compileToPinotQuery("SET queryOption1='foo'; select * from testTable");
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption1"), "foo");

    // Has query options in json payload
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=foo");
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption1"), "foo");

    // Has query options in both json payload and pinotQuery, pinotQuery takes priority
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    pinotQuery = CalciteSqlParser.compileToPinotQuery("SET queryOption1='foo'; select * from testTable;");
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertNull(pinotQuery.getDebugOptions());
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 2 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption1"), "foo");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption2"), "moo");

    // Has all 3
    jsonRequest = JsonUtils.newObjectNode();
    jsonRequest.put(Request.TRACE, true);
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.setOptions(pinotQuery, requestId, query, jsonRequest);
    Assert.assertEquals(pinotQuery.getDebugOptionsSize(), 1);
    Assert.assertEquals(pinotQuery.getDebugOptions().get("debugOption1"), "foo");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 4 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption1"), "bar");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("queryOption2"), "moo");
    Assert.assertEquals(pinotQuery.getQueryOptions().get(Request.TRACE), "true");
    Assert.assertEquals(pinotQuery.getDebugOptions().get("debugOption1"), "foo");
  }
}

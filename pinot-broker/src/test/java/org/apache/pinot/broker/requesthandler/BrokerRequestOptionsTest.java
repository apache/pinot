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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the various options set in the broker request
 */
public class BrokerRequestOptionsTest {

  @Test
  public void testSetOptions() {
    Pql2Compiler compiler = new Pql2Compiler();

    BrokerRequest brokerRequest;
    ObjectNode jsonRequest;
    int requestId = 1;
    String query = "select * from table";

    // none of the options
    jsonRequest = JsonUtils.newObjectNode();
    brokerRequest = compiler.compileToBrokerRequest(query);
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertFalse(brokerRequest.isEnableTrace());
    Assert.assertNull(brokerRequest.getDebugOptions());
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 0);

    // TRACE
    // has trace false
    jsonRequest.put(Request.TRACE, false);
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertFalse(brokerRequest.isEnableTrace());
    Assert.assertNull(brokerRequest.getDebugOptions());
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 0);

    // has trace true
    jsonRequest.put(Request.TRACE, true);
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertTrue(brokerRequest.isEnableTrace());

    // DEBUG_OPTIONS
    // has debugOptions
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getDebugOptionsSize(), 1);
    Assert.assertEquals(brokerRequest.getDebugOptions().get("debugOption1"), "foo");

    // has multiple debugOptions
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo;debugOption2=bar");
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getDebugOptionsSize(), 2);
    Assert.assertEquals(brokerRequest.getDebugOptions().get("debugOption1"), "foo");
    Assert.assertEquals(brokerRequest.getDebugOptions().get("debugOption2"), "bar");

    // incorrect debug options
    boolean exception = false;
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1");
    try {
      BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // QUERY_OPTIONS
    jsonRequest = JsonUtils.newObjectNode();
    // has queryOptions in brokerRequest already
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("queryOption1", "foo");
    brokerRequest.setQueryOptions(queryOptions);
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 1);
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption1"), "foo");

    // has queryOptions in query
    query = "select * from table option(queryOption1=foo)";
    brokerRequest = compiler.compileToBrokerRequest(query);
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 1);
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption1"), "foo");

    // has query options in json payload
    query = "select * from table";
    brokerRequest = compiler.compileToBrokerRequest(query);
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=foo");
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 1);
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption1"), "foo");

    // has query options in both, union. broker request takes priority
    query = "select * from table option(queryOption1=foo)";
    brokerRequest = compiler.compileToBrokerRequest(query);
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 2);
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption1"), "foo");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption2"), "moo");

    // has all 3
    query = "select * from table";
    jsonRequest = JsonUtils.newObjectNode();
    brokerRequest = compiler.compileToBrokerRequest(query);
    jsonRequest.put(Request.TRACE, true);
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    BaseBrokerRequestHandler.setOptions(requestId, query, jsonRequest, brokerRequest);
    Assert.assertTrue(brokerRequest.isEnableTrace());
    Assert.assertEquals(brokerRequest.getDebugOptionsSize(), 1);
    Assert.assertEquals(brokerRequest.getDebugOptions().get("debugOption1"), "foo");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 2);
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption1"), "bar");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("queryOption2"), "moo");
  }
}

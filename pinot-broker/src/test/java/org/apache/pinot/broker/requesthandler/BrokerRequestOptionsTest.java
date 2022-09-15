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
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
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
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);;
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 0 + LEGACY_PQL_QUERY_OPTION_SIZE);

    // TRACE
    // Has trace false
    jsonRequest.put(Request.TRACE, false);
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 0 + LEGACY_PQL_QUERY_OPTION_SIZE);

    // Has trace true
    jsonRequest.put(Request.TRACE, true);
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get(Request.TRACE), "true");

    // DEBUG_OPTIONS (debug options will also be included as query options)
    // Has debugOptions
    jsonRequest = JsonUtils.newObjectNode();
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("debugOption1"), "foo");

    // Has multiple debugOptions
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo;debugOption2=bar");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 2 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("debugOption1"), "foo");
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("debugOption2"), "bar");

    // Invalid debug options
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    try {
      RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
      Assert.fail();
    } catch (Exception e) {
      // Expected
    }

    // QUERY_OPTIONS
    jsonRequest = JsonUtils.newObjectNode();
    // Has queryOptions in sqlNodeAndOptions already
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("queryOption1", "foo");
    sqlNodeAndOptions.getOptions().putAll(queryOptions);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption1"), "foo");

    // Has queryOptions in query
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions("SET queryOption1='foo'; select * from testTable");
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption1"), "foo");

    // Has query options in json payload
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=foo");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 1 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption1"), "foo");

    // Has query options in both json payload and sqlNodeAndOptions, sqlNodeAndOptions takes priority
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions("SET queryOption1='foo'; select * from testTable;");
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 2 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption1"), "foo");
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption2"), "moo");

    // Has all 3
    jsonRequest = JsonUtils.newObjectNode();
    jsonRequest.put(Request.TRACE, true);
    jsonRequest.put(Request.DEBUG_OPTIONS, "debugOption1=foo");
    jsonRequest.put(Request.QUERY_OPTIONS, "queryOption1=bar;queryOption2=moo");
    sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    RequestUtils.setOptions(sqlNodeAndOptions, jsonRequest);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().size(), 4 + LEGACY_PQL_QUERY_OPTION_SIZE);
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption1"), "bar");
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("queryOption2"), "moo");
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get(Request.TRACE), "true");
    Assert.assertEquals(sqlNodeAndOptions.getOptions().get("debugOption1"), "foo");
  }
}

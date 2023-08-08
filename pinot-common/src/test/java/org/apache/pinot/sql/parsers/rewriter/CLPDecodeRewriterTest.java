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
package org.apache.pinot.sql.parsers.rewriter;

import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class CLPDecodeRewriterTest {
  private static final QueryRewriter _QUERY_REWRITER = new CLPDecodeRewriter();

  @Test
  public void testCLPDecodeRewrite() {
    // clpDecode rewrite from column group to individual columns
    testQueryRewrite("SELECT clpDecode(message) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite("SELECT clpDecode(message, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");

    // clpDecode passthrough
    testQueryRewrite("SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite(
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");
  }

  @Test
  public void testUnsupportedCLPDecodeQueries() {
    testUnsupportedQuery("SELECT clpDecode('message') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', 'default') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', default) FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode(message, default) FROM clpTable");
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(_QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }

  private void testUnsupportedQuery(String query) {
     assertThrows(SqlCompilationException.class,
        () -> _QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query)));
  }
}

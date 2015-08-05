/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.pql.parsers;

import com.linkedin.pinot.common.request.BrokerRequest;
import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;

import com.linkedin.pinot.pql.parsers.PQLLexer;
import com.linkedin.pinot.pql.parsers.PQLParser;


public class PQLCompiler extends AbstractCompiler {
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<>();
  private ThreadLocal<PQLParser> _parser = new ThreadLocal<>();

  public PQLCompiler(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }

  @Override
  public JSONObject compile(String bqlStmt) throws RecognitionException {
    // Lexer splits input into tokens
    ANTLRStringStream input = new ANTLRStringStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new PQLLexer(input));

    // Parser generates abstract syntax tree
    PQLParser parser = new PQLParser(tokens, _facetInfoMap);
    _parser.set(parser);
    PQLParser.statement_return ret = parser.statement();

    // Acquire parse result
    CommonTree ast = (CommonTree) ret.tree;

    JSONObject json = (JSONObject) ret.json;
    // XXX To be removed
    // printTree(ast);
    // System.out.println(">>> json = " + json.toString());
    return json;
  }

  @Override
  public BrokerRequest compileToBrokerRequest(String expression) {
    return null;
  }

  @Override
  public String getErrorMessage(RecognitionException error) {
    PQLParser parser = _parser.get();
    if (parser != null) {
      return parser.getErrorMessage(error, parser.getTokenNames());
    } else {
      return null;
    }
  }

  public void setFacetInfoMap(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }
}

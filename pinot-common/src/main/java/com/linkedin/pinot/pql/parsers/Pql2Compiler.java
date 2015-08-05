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
import com.linkedin.pinot.pql.parsers.pql2.ast.AstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.BaseAstNode;
import org.antlr.runtime.RecognitionException;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.json.JSONObject;


/**
 * PQL 2 compiler.
 */
public class Pql2Compiler extends AbstractCompiler {
  @Override
  public JSONObject compile(String expression)
      throws RecognitionException {
    return null;
  }

  @Override
  public BrokerRequest compileToBrokerRequest(String expression) throws Pql2CompilationException {
    try {
      //
      CharStream charStream = new ANTLRInputStream(expression);
      PQL2Lexer lexer = new PQL2Lexer(charStream);
      lexer.setTokenFactory(new CommonTokenFactory(true));
      TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
      PQL2Parser parser = new PQL2Parser(tokenStream);
      parser.setErrorHandler(new BailErrorStrategy());

      // Parse
      ParseTree parseTree = parser.root();

      ParseTreeWalker walker = new ParseTreeWalker();
      Pql2AstListener listener = new Pql2AstListener();
      walker.walk(listener, parseTree);

      AstNode rootNode = listener.getRootNode();
      // System.out.println(rootNode.toString(0));

      BrokerRequest brokerRequest = new BrokerRequest();
      rootNode.updateBrokerRequest(brokerRequest);
      return brokerRequest;
    } catch (Pql2CompilationException e) {
      throw e;
    } catch (Exception e) {
      throw new Pql2CompilationException(e.getMessage());
    }
  }

  @Override
  public String getErrorMessage(RecognitionException error) {
    return null;
  }
}

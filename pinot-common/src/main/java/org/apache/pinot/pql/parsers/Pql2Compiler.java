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
package org.apache.pinot.pql.parsers;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.parsers.QueryCompiler;
import org.apache.pinot.parsers.utils.BrokerRequestComparisonUtils;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PQL 2 compiler.
 */
@ThreadSafe
public class Pql2Compiler implements QueryCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pql2Compiler.class);

  private static boolean _enablePinotQuery =
      Boolean.valueOf(System.getProperty("pinot.query.converter.enabled", "false"));
  private static boolean _validateConverter =
      Boolean.valueOf(System.getProperty("pinot.query.converter.validate", "false"));
  private static boolean _failOnConversionError =
      Boolean.valueOf(System.getProperty("pinot.query.converter.fail_on_error", "false"));

  @VisibleForTesting
  public static void setEnablePinotQuery(boolean enablePinotQuery) {
    _enablePinotQuery = enablePinotQuery;
  }

  @VisibleForTesting
  public static void setValidateConverter(boolean validateConverter) {
    _validateConverter = validateConverter;
  }

  @VisibleForTesting
  public static void setFailOnConversionError(boolean failOnConversionError) {
    _failOnConversionError = failOnConversionError;
  }

  private static class ErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(@Nonnull Recognizer<?, ?> recognizer, @Nullable Object offendingSymbol, int line,
        int charPositionInLine, @Nonnull String msg, @Nullable RecognitionException e) {
      throw new Pql2CompilationException(msg, offendingSymbol, line, charPositionInLine, e);
    }
  }

  private static final ErrorListener ERROR_LISTENER = new ErrorListener();

  /**
   * Compile the given expression into {@link BrokerRequest}.
   *
   * @param expression Expression to compile
   * @return BrokerRequest
   */
  @Override
  public BrokerRequest compileToBrokerRequest(String expression)
      throws Pql2CompilationException {
    try {
      //
      CharStream charStream = new ANTLRInputStream(expression);
      PQL2Lexer lexer = new PQL2Lexer(charStream);
      lexer.setTokenFactory(new CommonTokenFactory(true));
      lexer.removeErrorListeners();
      lexer.addErrorListener(ERROR_LISTENER);
      TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
      PQL2Parser parser = new PQL2Parser(tokenStream);
      parser.setErrorHandler(new BailErrorStrategy());
      parser.removeErrorListeners();
      parser.addErrorListener(ERROR_LISTENER);

      // Parse
      ParseTree parseTree = parser.root();

      ParseTreeWalker walker = new ParseTreeWalker();
      Pql2AstListener listener = new Pql2AstListener(expression);
      walker.walk(listener, parseTree);

      AstNode rootNode = listener.getRootNode();
      BrokerRequest brokerRequest = new BrokerRequest();
      rootNode.updateBrokerRequest(brokerRequest);
      if (_enablePinotQuery) {
        try {
          PinotQuery pinotQuery = new PinotQuery();
          rootNode.updatePinotQuery(pinotQuery);
          if (_validateConverter) {
            PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
            BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
            boolean result = BrokerRequestComparisonUtils.validate(brokerRequest, tempBrokerRequest);
            if (!result) {
              LOGGER.error("Pinot query to broker request conversion failed. PQL:{}", expression);
              if (_failOnConversionError) {
                throw new Pql2CompilationException(
                    "Pinot query to broker request conversion failed. PQL:" + expression);
              }
            }
          }
          brokerRequest.setPinotQuery(pinotQuery);
        } catch (Exception e) {
          //non fatal for now.
          LOGGER.error("Non fatal: Failed to populate pinot query and broker request. PQL:{}", expression, e);
          if (_failOnConversionError) {
            throw e;
          }
        }
      }
      return brokerRequest;
    } catch (Pql2CompilationException e) {
      throw e;
    } catch (Exception e) {
      throw new Pql2CompilationException(ExceptionUtils.getStackTrace(e));
    }
  }

  public AstNode parseToAstNode(String expression) {
    try {
      CharStream charStream = new ANTLRInputStream(expression);
      PQL2Lexer lexer = new PQL2Lexer(charStream);
      lexer.setTokenFactory(new CommonTokenFactory(true));
      TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
      PQL2Parser parser = new PQL2Parser(tokenStream);
      parser.setErrorHandler(new BailErrorStrategy());
      parser.removeErrorListeners();

      // Parse
      Pql2AstListener listener = new Pql2AstListener(expression);
      new ParseTreeWalker().walk(listener, parser.expression());
      return listener.getRootNode();
    } catch (Exception e) {
      // NOTE: Treat reserved keys as identifiers. E.g. '*', 'group', 'order', etc.
      return new IdentifierAstNode(expression);
    }
  }

  public TransformExpressionTree compileToExpressionTree(String expression) {
    return new TransformExpressionTree(parseToAstNode(expression));
  }

  public static AstNode buildAst(String expression) {
    CharStream charStream = new ANTLRInputStream(expression);
    PQL2Lexer lexer = new PQL2Lexer(charStream);
    lexer.setTokenFactory(new CommonTokenFactory(true));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ERROR_LISTENER);
    TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
    PQL2Parser parser = new PQL2Parser(tokenStream);
    parser.setErrorHandler(new BailErrorStrategy());
    parser.removeErrorListeners();
    parser.addErrorListener(ERROR_LISTENER);

    // Parse
    ParseTree parseTree = parser.root();

    ParseTreeWalker walker = new ParseTreeWalker();
    Pql2AstListener listener = new Pql2AstListener(expression);
    walker.walk(listener, parseTree);

    AstNode rootNode = listener.getRootNode();
    return rootNode;
  }
}

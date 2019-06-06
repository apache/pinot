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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
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
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BaseAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BetweenPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.ComparisonPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.HavingAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.InPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OutputColumnAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.RegexpLikePredicateAstNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PQL 2 compiler.
 */
@ThreadSafe
public class Pql2Compiler implements AbstractCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pql2Compiler.class);

  private static final boolean VALIDATE_CONVERTER =
      Boolean.valueOf(System.getProperty("pinot.pql.validate.converter", "false"));

  private static class ErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(@Nonnull Recognizer<?, ?> recognizer, @Nullable Object offendingSymbol,
        int line, int charPositionInLine, @Nonnull String msg, @Nullable RecognitionException e) {
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
      //Validate the HAVING clause if any
      validateHavingClause(rootNode);

      BrokerRequest brokerRequest = new BrokerRequest();
      rootNode.updateBrokerRequest(brokerRequest);
      try {
        PinotQuery pinotQuery = new PinotQuery();
        rootNode.updatePinotQuery(pinotQuery);
        if (VALIDATE_CONVERTER) {
          PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
          BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
          boolean result = validate(brokerRequest, tempBrokerRequest);
          if (!result) {
            LOGGER.error("Pinot query to broker request conversion failed. PQL:{}", expression);
          }
        }
        brokerRequest.setPinotQuery(pinotQuery);
      } catch (Exception e) {
        //non fatal for now.
        LOGGER.error("Non fatal: Failed to populate pinot query and broker request. PQL:{}",
            expression, e);
      }
      return brokerRequest;
    } catch (Pql2CompilationException e) {
      throw e;
    } catch (Exception e) {
      throw new Pql2CompilationException(ExceptionUtils.getStackTrace(e));
    }
  }

  private boolean validate(BrokerRequest br1, BrokerRequest br2) throws Exception {
    //Having not yet supported
    if (br1.getHavingFilterQuery() != null) {
      return true;
    }
    boolean result = br1.equals(br2);
    if (!result) {
      StringBuilder sb = new StringBuilder();

      if (br1.getFilterQuery() != null) {
        if (!br1.getFilterQuery().equals(br2.getFilterQuery())) {
          sb.append("br1.getFilterQuery() = ").append(br1.getFilterQuery()).append("\n")
              .append("br2.getFilterQuery() = ").append(br2.getFilterQuery());
          LOGGER.error("Filter did not match after conversion.{}", sb);
          return false;
        }

        if (!br1.getFilterSubQueryMap().equals(br2.getFilterSubQueryMap())) {
          sb.append("br1.getFilterSubQueryMap() = ").append(br1.getFilterSubQueryMap()).append("\n")
              .append("br2.getFilterSubQueryMap() = ").append(br2.getFilterSubQueryMap());
          LOGGER.error("FilterSubQueryMap did not match after conversion. {}", sb);
          return false;
        }
      }
      if (br1.getSelections() != null) {
        if (!br1.getSelections().equals(br2.getSelections())) {
          sb.append("br1.getSelections() = ").append(br1.getSelections()).append("\n")
              .append("br2.getSelections() = ").append(br2.getSelections());
          LOGGER.error("Selection did not match after conversion:{}", sb);
          return false;
        }
      }
      if (br1.getGroupBy() != null) {
        if (!br1.getGroupBy().equals(br2.getGroupBy())) {
          sb.append("br1.getGroupBy() = ").append(br1.getGroupBy()).append("\n")
              .append("br2.getGroupBy() = ").append(br2.getGroupBy());
          LOGGER.error("Group By did not match conversion:{}", sb);
          return false;
        }
      }
      if (br1.getAggregationsInfo() != null) {
        List<AggregationInfo> aggregationsInfo = br1.getAggregationsInfo();
        for (int i = 0; i < aggregationsInfo.size(); i++) {
          AggregationInfo agg1 = br1.getAggregationsInfo().get(i);
          AggregationInfo agg2 = br2.getAggregationsInfo().get(i);
          if (!agg1.equals(agg2)) {
            sb.append("br1.agg1 = ").append(agg1).append("\n")
                .append("br2.agg2() = ").append(agg2);
            LOGGER.error("AggregationInfo did not match after conversion: {}", sb);
            return false;
          }
        }
      }
    }
    return result;
  }


  @Override
  public TransformExpressionTree compileToExpressionTree(String expression) {
    CharStream charStream = new ANTLRInputStream(expression);
    PQL2Lexer lexer = new PQL2Lexer(charStream);
    lexer.setTokenFactory(new CommonTokenFactory(true));
    TokenStream tokenStream = new UnbufferedTokenStream<CommonToken>(lexer);
    PQL2Parser parser = new PQL2Parser(tokenStream);
    parser.setErrorHandler(new BailErrorStrategy());

    // Parse
    ParseTree parseTree = parser.expression();

    ParseTreeWalker walker = new ParseTreeWalker();
    Pql2AstListener listener = new Pql2AstListener(expression);
    walker.walk(listener, parseTree);

    return new TransformExpressionTree(listener.getRootNode());
  }

  private void validateHavingClause(AstNode rootNode) {
    List<? extends AstNode> children = rootNode.getChildren();
    BaseAstNode outList = (BaseAstNode) children.get(0);
    HavingAstNode havingList = null;
    boolean isThereHaving = false;

    for (int i = 1; i < children.size(); i++) {
      if (children.get(i) instanceof HavingAstNode) {
        havingList = (HavingAstNode) children.get(i);
        isThereHaving = true;
        break;
      }
    }

    if (isThereHaving) {
      // Check if the HAVING predicate function call is in the select list;
      // if not: add the missing function call to select list and set isInSelectList to false
      List<FunctionCallAstNode> functionCalls = havingTreeDFSTraversalToFindFunctionCalls(
          havingList);

      if (functionCalls.isEmpty()) {
        throw new Pql2CompilationException(
            "HAVING clause needs to have minimum one function call comparison");
      }

      List<? extends AstNode> outListChildren = outList.getChildren();
      for (FunctionCallAstNode havingFunction : functionCalls) {
        boolean functionCallIsInSelectList = false;
        for (AstNode anOutListChildren : outListChildren) {
          OutputColumnAstNode selectItem = (OutputColumnAstNode) anOutListChildren;
          if (selectItem.getChildren().get(0) instanceof FunctionCallAstNode) {
            FunctionCallAstNode function = (FunctionCallAstNode) selectItem.getChildren().get(0);
            if (function.getExpression().equalsIgnoreCase(havingFunction.getExpression())
                && function.getName()
                .equalsIgnoreCase(havingFunction.getName())) {
              functionCallIsInSelectList = true;
              break;
            }
          }
        }

        if (!functionCallIsInSelectList) {
          OutputColumnAstNode havingFunctionAstNode = new OutputColumnAstNode();
          havingFunction.setIsInSelectList(false);
          havingFunctionAstNode.addChild(havingFunction);
          havingFunction.setParent(havingFunctionAstNode);
          outList.addChild(havingFunctionAstNode);
          havingFunctionAstNode.setParent(outList);
        }
      }
    }
  }

  private List<FunctionCallAstNode> havingTreeDFSTraversalToFindFunctionCalls(
      HavingAstNode havingList) {
    List<FunctionCallAstNode> functionCalls = new ArrayList<>();
    Stack<AstNode> astNodeStack = new Stack<>();
    astNodeStack.add(havingList);
    while (!astNodeStack.isEmpty()) {
      AstNode visitingNode = astNodeStack.pop();
      if (visitingNode instanceof ComparisonPredicateAstNode) {
        if (!((ComparisonPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        if (!NumberUtils.isNumber(((ComparisonPredicateAstNode) visitingNode).getValue())) {
          throw new Pql2CompilationException(
              "Having clause only supports comparing function result with numbers");
        }
        functionCalls.add(((ComparisonPredicateAstNode) visitingNode).getFunction());
      } else if (visitingNode instanceof BetweenPredicateAstNode) {
        if (!((BetweenPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        if (!NumberUtils.isNumber(((BetweenPredicateAstNode) visitingNode).getLeftValue())) {
          throw new Pql2CompilationException(
              "Having clause only supports comparing function result with numbers");
        }
        if (!NumberUtils.isNumber(((BetweenPredicateAstNode) visitingNode).getRightValue())) {
          throw new Pql2CompilationException(
              "Having clause only supports comparing function result with numbers");
        }
        functionCalls.add(((BetweenPredicateAstNode) visitingNode).getFunction());
      } else if (visitingNode instanceof InPredicateAstNode) {
        if (!((InPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        for (String value : ((InPredicateAstNode) visitingNode).getValues()) {
          if (!NumberUtils.isNumber(value)) {
            throw new Pql2CompilationException(
                "Having clause only supports comparing function result with numbers");
          }
        }
        functionCalls.add(((InPredicateAstNode) visitingNode).getFunction());
      } else if (visitingNode instanceof RegexpLikePredicateAstNode) {
        throw new Pql2CompilationException("Having predicate does not support regular expression");
      } else {
        if (visitingNode.hasChildren()) {
          for (AstNode children : visitingNode.getChildren()) {
            astNodeStack.add(children);
          }
        }
      }
    }
    return functionCalls;
  }
}

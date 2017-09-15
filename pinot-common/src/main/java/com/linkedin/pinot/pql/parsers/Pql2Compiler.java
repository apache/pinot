/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.pql.parsers.pql2.ast.AstNode;

import com.linkedin.pinot.pql.parsers.pql2.ast.BaseAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.BetweenPredicateAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.ComparisonPredicateAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.HavingAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.InPredicateAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.OutputColumnAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.RegexpLikePredicateAstNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
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
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.misc.Nullable;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;


/**
 * PQL 2 compiler.
 */
public class Pql2Compiler implements AbstractCompiler {
  private boolean _splitInClause = false;

  private static class ErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(@NotNull Recognizer<?, ?> recognizer, @Nullable Object offendingSymbol, int line,
        int charPositionInLine, @NotNull String msg, @Nullable RecognitionException e) {
      throw new Pql2CompilationException(msg, offendingSymbol, line, charPositionInLine, e);
    }
  }

  private static final ErrorListener ERROR_LISTENER = new ErrorListener();

  @Override
  public BrokerRequest compileToBrokerRequest(String expression) throws Pql2CompilationException {
    return compileToBrokerRequest(expression, false);
  }

  /**
   * Compile the given expression into {@link BrokerRequest}.
   *
   * @param expression Expression to compile
   * @param splitInClause Value of in clause sent as list if true, joined with delimiter otherwise. This is a temporary
   *                      argument to keep the broker and server compatible, and will be removed.
   * @return BrokerRequest
   * @throws Pql2CompilationException
   */
  public BrokerRequest compileToBrokerRequest(String expression, boolean splitInClause) throws Pql2CompilationException {
    _splitInClause = splitInClause;
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
      Pql2AstListener listener = new Pql2AstListener(expression, _splitInClause);
      walker.walk(listener, parseTree);

      AstNode rootNode = listener.getRootNode();
      //Validate the HAVING clause if any
      validateHavingClause(rootNode);

      BrokerRequest brokerRequest = new BrokerRequest();
      rootNode.updateBrokerRequest(brokerRequest);
      return brokerRequest;
    } catch (Pql2CompilationException e) {
      throw e;
    } catch (Exception e) {
      throw new Pql2CompilationException(ExceptionUtils.getStackTrace(e));
    }
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
    Pql2AstListener listener = new Pql2AstListener(expression, _splitInClause);
    walker.walk(listener, parseTree);

    final AstNode rootNode = listener.getRootNode();
    return TransformExpressionTree.buildTree(rootNode);
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
      List<FunctionCallAstNode> functionCalls = havingTreeDFSTraversalToFindFunctionCalls(havingList);

      if (functionCalls.isEmpty()) {
        throw new Pql2CompilationException("HAVING clause needs to have minimum one function call comparison");
      }

      List<? extends AstNode> outListChildren = outList.getChildren();
      for (FunctionCallAstNode havingFunction : functionCalls) {
        boolean functionCallIsInSelectList = false;
        for (AstNode anOutListChildren : outListChildren) {
          OutputColumnAstNode selectItem = (OutputColumnAstNode) anOutListChildren;
          if (selectItem.getChildren().get(0) instanceof FunctionCallAstNode) {
            FunctionCallAstNode function = (FunctionCallAstNode) selectItem.getChildren().get(0);
            if (function.getExpression().equalsIgnoreCase(havingFunction.getExpression()) && function.getName()
                .equalsIgnoreCase(havingFunction.getName())) {
              functionCallIsInSelectList = true;
              break;
            }
          }
        }

        if (functionCallIsInSelectList == false) {
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

  private List<FunctionCallAstNode> havingTreeDFSTraversalToFindFunctionCalls(HavingAstNode havingList) {
    List<FunctionCallAstNode> functionCalls = new ArrayList<FunctionCallAstNode>();
    Stack<AstNode> astNodeStack = new Stack<AstNode>();
    astNodeStack.add(havingList);
    while (!astNodeStack.isEmpty()) {
      AstNode visitingNode = astNodeStack.pop();
      if (visitingNode instanceof ComparisonPredicateAstNode) {
        if (!((ComparisonPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        if (!NumberUtils.isNumber(((ComparisonPredicateAstNode) visitingNode).getValue())) {
          throw new Pql2CompilationException("Having clause only supports comparing function result with numbers");
        }
        functionCalls.add(((ComparisonPredicateAstNode) visitingNode).getFunction());
      } else if (visitingNode instanceof BetweenPredicateAstNode) {
        if (!((BetweenPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        if (!NumberUtils.isNumber(((BetweenPredicateAstNode) visitingNode).getLeftValue())) {
          throw new Pql2CompilationException("Having clause only supports comparing function result with numbers");
        }
        if (!NumberUtils.isNumber(((BetweenPredicateAstNode) visitingNode).getRightValue())) {
          throw new Pql2CompilationException("Having clause only supports comparing function result with numbers");
        }
        functionCalls.add(((BetweenPredicateAstNode) visitingNode).getFunction());
      } else if (visitingNode instanceof InPredicateAstNode) {
        if (!((InPredicateAstNode) visitingNode).isItFunctionCallComparison()) {
          throw new Pql2CompilationException("Having predicate only compares function calls");
        }
        for (String value : ((InPredicateAstNode) visitingNode).getValues()) {
          if (!NumberUtils.isNumber(value)) {
            throw new Pql2CompilationException("Having clause only supports comparing function result with numbers");
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

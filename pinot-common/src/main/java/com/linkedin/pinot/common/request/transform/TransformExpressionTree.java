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
package com.linkedin.pinot.common.request.transform;

import com.linkedin.pinot.pql.parsers.pql2.ast.AstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import java.util.ArrayList;
import java.util.List;


/**
 * Class for representing expression trees for transforms.
 * <p> - A TransformExpressionTree node has either transform function or a column name, or a literal.</p>
 * <p> - Leaf nodes either have column name or literal, whereas non-leaf nodes have transform function.</p>
 * <p> - Transform function in non-leaf nodes is applied to its children nodes. </p>
 */
public class TransformExpressionTree {

  // Enum for expression represented by the tree.
  public enum ExpressionType {
    FUNCTION,
    IDENTIFIER,
    LITERAL
  }

  private final String _expression;
  private final String _transformName;
  private final ExpressionType _expressionType;
  private List<TransformExpressionTree> _children;

  /**
   * Constructor for the class
   * @param root Root AstNode for the tree
   */
  public TransformExpressionTree(AstNode root) {
    _children = null;

    if (root instanceof FunctionCallAstNode) {
      FunctionCallAstNode functionCallAstNode = (FunctionCallAstNode) root;
      _expression = functionCallAstNode.getExpression();
      _expressionType = ExpressionType.FUNCTION;
      _transformName = functionCallAstNode.getName();

    } else if (root instanceof IdentifierAstNode) {
      _expression = ((IdentifierAstNode) root).getName();
      _expressionType = ExpressionType.IDENTIFIER;
      _transformName = null;

    } else if (root instanceof LiteralAstNode) {
      _expressionType = ExpressionType.LITERAL;
      _expression = ((LiteralAstNode) root).getValueAsString();
      _transformName = null;

    } else {
      throw new IllegalArgumentException(
          "Illegal AstNode type for TransformExpressionTree: " + root.getClass().getName());
    }
  }

  /**
   * Given the root node for AST tree, builds the expression tree, and returns
   * the root node of the expression tree.
   *
   * @param rootNode Root Node of AST tree
   * @return Root node of the Expression tree
   */
  public static TransformExpressionTree buildTree(AstNode rootNode) {
    TransformExpressionTree expressionTree;

    if (!rootNode.hasChildren()) {
      return new TransformExpressionTree(rootNode);
    } else {
      expressionTree = new TransformExpressionTree(rootNode);
      expressionTree._children = new ArrayList<>();

      for (AstNode child : rootNode.getChildren()) {
        expressionTree._children.add(buildTree(child));
      }
    }
    return expressionTree;
  }

  /**
   * Getter for the expression string.
   *
   * @return Expression string name
   */
  @Override
  public String toString() {
    return _expression;
  }

  /**
   * Getter for transform function name.
   * @return transform function name
   */
  public String getTransformName() {
    return _transformName;
  }


  /**
   * Getter for children of current node.
   * @return children of current node
   */
  public List<TransformExpressionTree> getChildren() {
    return _children;
  }

  /**
   * Returns the Expression type, which can be one of the following:
   * <ul>
   *   <li> {@link ExpressionType#FUNCTION}</li>
   *   <li> {@link ExpressionType#IDENTIFIER}</li>
   *   <li> {@link ExpressionType#LITERAL}</li>
   * </ul>
   * @return Expression type.
   */
  public ExpressionType getExpressionType() {
    return _expressionType;
  }

  /**
   * Returns if the tree represents a column name (ie no expression), false otherwise
   *
   * @return True if tress represents column, false otherwise.
   */
  public boolean isColumn() {
    return (_expressionType == ExpressionType.IDENTIFIER);
  }

  /**
   * Returns a list of columns from the leaf nodes of the tree.
   * Caller passes a list where columns are stored and returned.
   *
   * @param columns Output columns
   */
  public void getColumns(List<String> columns) {

    if (_children == null || _children.isEmpty()) {
      if (_expressionType == ExpressionType.IDENTIFIER) {
        columns.add(_expression);
      }
      return;
    }

    for (TransformExpressionTree child : _children) {
      child.getColumns(columns);
    }
  }
}

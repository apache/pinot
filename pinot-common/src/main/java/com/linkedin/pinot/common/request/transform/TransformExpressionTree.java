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

import com.linkedin.pinot.pql.parsers.Pql2CompilationException;
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
  private final String _column;
  private final String _literal;
  private final TransformFunction _transform;
  private final List<TransformExpressionTree> _children;

  /**
   * Constructor for the class
   * @param column Column on which to apply the transform.
   * @param literal A constant value, to be used as an argument for the transform function
   * @param transformName Name of transform function to apply
   * @param children Children nodes for the current node
   */
  public TransformExpressionTree(String column, String literal, String transformName,
      List<TransformExpressionTree> children) {
    _column = column;
    _literal = literal;
    _transform = (transformName != null) ? TransformFunctionFactory.get(transformName) : null;
    _children = children;

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
      if (rootNode instanceof IdentifierAstNode) {
        return new TransformExpressionTree(((IdentifierAstNode) rootNode).getName(), null, null, null);
      } else if (rootNode instanceof LiteralAstNode) {
        return new TransformExpressionTree(null, ((LiteralAstNode) rootNode).getValueAsString(), null, null);
      } else {
        throw new Pql2CompilationException(
            "Leaf of expression tree can only be identifier or literal, found " + rootNode.getClass().getName());
      }
    } else if (rootNode instanceof FunctionCallAstNode) {
      List<TransformExpressionTree> children = new ArrayList<>();
      expressionTree =
          new TransformExpressionTree(null, null, ((FunctionCallAstNode) rootNode).getName(), children);
      for (AstNode child : rootNode.getChildren()) {
        children.add(buildTree(child));
      }
    } else {
      throw new Pql2CompilationException(
          "Expression tree node can only be function, identifier, or literal, found " + rootNode.getClass().getName());
    }
    return expressionTree;
  }

  /**
   * Getter for the column name.
   * @return column name
   */
  public String getColumn() {
    return _column;
  }

  /**
   * Getter for value.
   * @return value
   */
  public String getLiteral() {
    return _literal;
  }

  /**
   * Getter for transform function name.
   * @return transform function name
   */
  public TransformFunction getTransform() {
    return _transform;
  }

  /**
   * Getter for children of current node.
   * @return children of current node
   */
  public List<TransformExpressionTree> getChildren() {
    return _children;
  }
}

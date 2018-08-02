/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.pql.parsers.pql2.ast.AstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import com.linkedin.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Class for representing expression trees for transforms.
 * <ul>
 *   <li>A TransformExpressionTree node has either transform function or a column name, or a literal.</li>
 *   <li>Leaf nodes either have column name or literal, whereas non-leaf nodes have transform function.</li>
 *   <li>Transform function in non-leaf nodes is applied to its children nodes.</li>
 * </ul>
 */
public class TransformExpressionTree {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  public static TransformExpressionTree compileToExpressionTree(String expression) {
    return COMPILER.compileToExpressionTree(expression);
  }

  /**
   * Compiles the expression and serializes it back to standard format.
   * <p>E.g. "  foo\t  ( bar  ('a'\t ,foobar(  b,  'c'\t, 123)  )   ,d  )\t" -> "foo(bar('a',foobar(b,'c','123')),d)"
   * <p>The standard format expressions will be used in the broker response.
   */
  public static String standardizeExpression(String expression) {
    return compileToExpressionTree(expression).toString();
  }

  /**
   * Converts an {@link AstNode} into a standard expression.
   */
  public static String getStandardExpression(AstNode astNode) {
    if (astNode instanceof IdentifierAstNode) {
      // Column name
      return ((IdentifierAstNode) astNode).getName();
    } else if (astNode instanceof FunctionCallAstNode) {
      // UDF expression
      return standardizeExpression(((FunctionCallAstNode) astNode).getExpression());
    } else if (astNode instanceof StringLiteralAstNode) {
      // Treat string as column name
      // NOTE: this is for backward-compatibility
      return ((StringLiteralAstNode) astNode).getText();
    } else {
      throw new IllegalStateException("Cannot get standard expression from " + astNode.getClass().getSimpleName());
    }
  }

  // Enum for expression represented by the tree.
  public enum ExpressionType {
    FUNCTION, IDENTIFIER, LITERAL
  }

  private final ExpressionType _expressionType;
  private final String _value;
  private final List<TransformExpressionTree> _children;

  public TransformExpressionTree(AstNode root) {
    if (root instanceof FunctionCallAstNode) {
      _expressionType = ExpressionType.FUNCTION;
      _value = ((FunctionCallAstNode) root).getName().toLowerCase();
      _children = new ArrayList<>();
      for (AstNode child : root.getChildren()) {
        _children.add(new TransformExpressionTree(child));
      }
    } else if (root instanceof IdentifierAstNode) {
      _expressionType = ExpressionType.IDENTIFIER;
      _value = ((IdentifierAstNode) root).getName();
      _children = null;
    } else if (root instanceof LiteralAstNode) {
      _expressionType = ExpressionType.LITERAL;
      _value = ((LiteralAstNode) root).getValueAsString();
      _children = null;
    } else {
      throw new IllegalArgumentException(
          "Illegal AstNode type for TransformExpressionTree: " + root.getClass().getName());
    }
  }

  /**
   * Returns the expression type of the node, which can be one of the following:
   * <ul>
   *   <li> {@link ExpressionType#FUNCTION}</li>
   *   <li> {@link ExpressionType#IDENTIFIER}</li>
   *   <li> {@link ExpressionType#LITERAL}</li>
   * </ul>
   *
   * @return Expression type
   */
  public ExpressionType getExpressionType() {
    return _expressionType;
  }

  /**
   * Returns the value of the node.
   *
   * @return Function name for FUNCTION; column name for IDENTIFIER; string value for LITERAL
   */
  public String getValue() {
    return _value;
  }

  /**
   * Returns the children of the node.
   *
   * @return List of children
   */
  public List<TransformExpressionTree> getChildren() {
    return _children;
  }

  /**
   * Returns if the tree represents a column name (ie no expression), false otherwise
   *
   * @return True if tress represents column, false otherwise.
   */
  public boolean isColumn() {
    return _expressionType == ExpressionType.IDENTIFIER;
  }

  /**
   * Add all columns to the passed in column set.
   *
   * @param columns Output columns
   */
  public void getColumns(@Nonnull Set<String> columns) {
    if (_expressionType == ExpressionType.IDENTIFIER) {
      columns.add(_value);
    } else if (_children != null) {
      for (TransformExpressionTree child : _children) {
        child.getColumns(columns);
      }
    }
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(EqualityUtils.hashCodeOf(_expressionType.hashCode(), _value), _children);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TransformExpressionTree) {
      TransformExpressionTree that = (TransformExpressionTree) obj;
      return _expressionType == that._expressionType && _value.equals(that._value) && Objects.equals(_children,
          that._children);
    }
    return false;
  }

  @Override
  public String toString() {
    switch (_expressionType) {
      case FUNCTION:
        StringBuilder builder = new StringBuilder(_value).append('(');
        int numChildren = _children.size();
        for (int i = 0; i < numChildren; i++) {
          builder.append(_children.get(i).toString());
          if (i != numChildren - 1) {
            builder.append(',');
          } else {
            builder.append(')');
          }
        }
        return builder.toString();
      case IDENTIFIER:
        return _value;
      case LITERAL:
        return "\'" + _value + "\'";
      default:
        throw new IllegalStateException();
    }
  }
}

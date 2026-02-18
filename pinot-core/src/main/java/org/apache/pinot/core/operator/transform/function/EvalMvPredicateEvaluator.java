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
package org.apache.pinot.core.operator.transform.function;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public final class EvalMvPredicateEvaluator {
  private static final EnumSet<Predicate.Type> SUPPORTED_PREDICATE_TYPES =
      EnumSet.of(Predicate.Type.EQ, Predicate.Type.NOT_EQ, Predicate.Type.IN, Predicate.Type.NOT_IN,
          Predicate.Type.RANGE, Predicate.Type.REGEXP_LIKE);

  private final EvalNode _root;

  private EvalMvPredicateEvaluator(EvalNode root) {
    _root = root;
  }

  public static EvalMvPredicateEvaluator forPredicate(String predicate, DataType dataType,
      @Nullable Dictionary dictionary, @Nullable String expectedColumn) {
    if (StringUtils.isBlank(predicate)) {
      throw new IllegalArgumentException("evalMv predicate must be a non-empty string");
    }
    FilterContext filterContext = parseFilterContext(predicate);
    ColumnReferenceTracker tracker = new ColumnReferenceTracker(expectedColumn);
    validateFilter(filterContext, tracker);
    EvalNode root = buildNode(filterContext, dictionary, dataType);
    return new EvalMvPredicateEvaluator(root);
  }

  private static FilterContext parseFilterContext(String predicate) {
    Expression expression = CalciteSqlParser.compileToExpression(predicate);
    ExpressionContext expressionContext = RequestContextUtils.getExpression(expression);
    return RequestContextUtils.getFilter(expressionContext);
  }

  private static void validateFilter(FilterContext filterContext, ColumnReferenceTracker tracker) {
    switch (filterContext.getType()) {
      case CONSTANT:
        return;
      case PREDICATE:
        Predicate predicate = filterContext.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() != ExpressionContext.Type.IDENTIFIER) {
          throw new IllegalArgumentException(
              "evalMv only supports predicates on a single column without functions");
        }
        tracker.record(lhs.getIdentifier());
        return;
      case AND:
      case OR:
        for (FilterContext child : filterContext.getChildren()) {
          validateFilter(child, tracker);
        }
        return;
      case NOT:
        validateFilter(filterContext.getChildren().get(0), tracker);
        return;
      default:
        throw new IllegalStateException("Unsupported filter type: " + filterContext.getType());
    }
  }

  private static EvalNode buildNode(FilterContext filterContext, @Nullable Dictionary dictionary, DataType dataType) {
    switch (filterContext.getType()) {
      case CONSTANT:
        return EvalNode.constant(filterContext.isConstantTrue());
      case PREDICATE:
        Predicate predicate = filterContext.getPredicate();
        if (!SUPPORTED_PREDICATE_TYPES.contains(predicate.getType())) {
          throw new IllegalArgumentException(
              "evalMv does not support predicate type: " + predicate.getType());
        }
        PredicateEvaluator evaluator =
            PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dictionary, dataType);
        if (evaluator.isAlwaysTrue()) {
          return EvalNode.constant(true);
        }
        if (evaluator.isAlwaysFalse()) {
          return EvalNode.constant(false);
        }
        return EvalNode.predicate(evaluator);
      case AND: {
        List<EvalNode> children = new ArrayList<>();
        for (FilterContext child : filterContext.getChildren()) {
          EvalNode node = buildNode(child, dictionary, dataType);
          if (node.isConstantFalse()) {
            return EvalNode.constant(false);
          }
          if (!node.isConstantTrue()) {
            children.add(node);
          }
        }
        if (children.isEmpty()) {
          return EvalNode.constant(true);
        }
        if (children.size() == 1) {
          return children.get(0);
        }
        return EvalNode.and(children);
      }
      case OR: {
        List<EvalNode> children = new ArrayList<>();
        for (FilterContext child : filterContext.getChildren()) {
          EvalNode node = buildNode(child, dictionary, dataType);
          if (node.isConstantTrue()) {
            return EvalNode.constant(true);
          }
          if (!node.isConstantFalse()) {
            children.add(node);
          }
        }
        if (children.isEmpty()) {
          return EvalNode.constant(false);
        }
        if (children.size() == 1) {
          return children.get(0);
        }
        return EvalNode.or(children);
      }
      case NOT: {
        EvalNode child = buildNode(filterContext.getChildren().get(0), dictionary, dataType);
        if (child.isConstant()) {
          return EvalNode.constant(!child.getConstantValue());
        }
        return EvalNode.not(child);
      }
      default:
        throw new IllegalStateException("Unsupported filter type: " + filterContext.getType());
    }
  }

  public boolean matchesDictId(int dictId) {
    return evalDictId(_root, dictId);
  }

  public boolean matchesInt(int value) {
    return evalInt(_root, value);
  }

  public boolean matchesLong(long value) {
    return evalLong(_root, value);
  }

  public boolean matchesFloat(float value) {
    return evalFloat(_root, value);
  }

  public boolean matchesDouble(double value) {
    return evalDouble(_root, value);
  }

  public boolean matchesString(@Nullable String value) {
    if (value == null) {
      return false;
    }
    return evalString(_root, value);
  }

  public boolean matchesBytes(@Nullable byte[] value) {
    if (value == null) {
      return false;
    }
    return evalBytes(_root, value);
  }

  private static boolean evalDictId(EvalNode node, int dictId) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(dictId);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalDictId(child, dictId)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalDictId(child, dictId)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalDictId(node._child, dictId);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalInt(EvalNode node, int value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalInt(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalInt(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalInt(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalLong(EvalNode node, long value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalLong(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalLong(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalLong(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalFloat(EvalNode node, float value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalFloat(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalFloat(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalFloat(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalDouble(EvalNode node, double value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalDouble(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalDouble(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalDouble(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalString(EvalNode node, String value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalString(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalString(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalString(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static boolean evalBytes(EvalNode node, byte[] value) {
    switch (node._type) {
      case CONSTANT:
        return node._constant;
      case PREDICATE:
        return node._evaluator.applySV(value);
      case AND:
        for (EvalNode child : node._children) {
          if (!evalBytes(child, value)) {
            return false;
          }
        }
        return true;
      case OR:
        for (EvalNode child : node._children) {
          if (evalBytes(child, value)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evalBytes(node._child, value);
      default:
        throw new IllegalStateException("Unsupported node type: " + node._type);
    }
  }

  private static final class ColumnReferenceTracker {
    @Nullable
    private final String _expectedColumn;
    private String _observedColumn;

    private ColumnReferenceTracker(@Nullable String expectedColumn) {
      _expectedColumn = expectedColumn;
    }

    private void record(String column) {
      if (_expectedColumn != null) {
        if (!StringUtils.equalsIgnoreCase(_expectedColumn, column)) {
          throw new IllegalArgumentException(
              "evalMv predicate must reference column '" + _expectedColumn + "', but found '" + column + "'");
        }
        return;
      }
      if (_observedColumn == null) {
        _observedColumn = column;
      } else if (!StringUtils.equalsIgnoreCase(_observedColumn, column)) {
        throw new IllegalArgumentException(
            "evalMv predicate must reference a single column, but found '" + _observedColumn + "' and '" + column
                + "'");
      }
    }
  }

  private static final class EvalNode {
    private enum Type {
      CONSTANT,
      PREDICATE,
      AND,
      OR,
      NOT
    }

    private final Type _type;
    private final boolean _constant;
    private final PredicateEvaluator _evaluator;
    private final List<EvalNode> _children;
    private final EvalNode _child;

    private EvalNode(Type type, boolean constant, PredicateEvaluator evaluator, List<EvalNode> children,
        EvalNode child) {
      _type = type;
      _constant = constant;
      _evaluator = evaluator;
      _children = children;
      _child = child;
    }

    private static EvalNode constant(boolean value) {
      return new EvalNode(Type.CONSTANT, value, null, null, null);
    }

    private static EvalNode predicate(PredicateEvaluator evaluator) {
      return new EvalNode(Type.PREDICATE, false, evaluator, null, null);
    }

    private static EvalNode and(List<EvalNode> children) {
      return new EvalNode(Type.AND, false, null, children, null);
    }

    private static EvalNode or(List<EvalNode> children) {
      return new EvalNode(Type.OR, false, null, children, null);
    }

    private static EvalNode not(EvalNode child) {
      return new EvalNode(Type.NOT, false, null, null, child);
    }

    private boolean isConstant() {
      return _type == Type.CONSTANT;
    }

    private boolean isConstantTrue() {
      return _type == Type.CONSTANT && _constant;
    }

    private boolean isConstantFalse() {
      return _type == Type.CONSTANT && !_constant;
    }

    private boolean getConstantValue() {
      return _constant;
    }
  }
}

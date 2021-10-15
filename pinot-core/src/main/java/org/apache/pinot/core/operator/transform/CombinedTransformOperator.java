package org.apache.pinot.core.operator.transform;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class CombinedTransformOperator<T> {
  private static final String OPERATOR_NAME = "TransformOperator";

  protected final Map<T, ProjectionOperator> _projectionOperatorMap;
  protected final Map<ExpressionContext, TransformFunction> _transformFunctionMap = new HashMap<>();

  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressions Collection of expressions to evaluate
   */
  public CombinedTransformOperator(Map<T, ProjectionOperator> projectionOperatorMap,
      Map<T, Collection<ExpressionContext>> expressions) {
    _projectionOperatorMap = projectionOperatorMap;
    _dataSourceMap = projectionOperator.getDataSourceMap();
    for (ExpressionContext expression : expressions) {
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      _transformFunctionMap.put(expression, transformFunction);
    }
  }
}

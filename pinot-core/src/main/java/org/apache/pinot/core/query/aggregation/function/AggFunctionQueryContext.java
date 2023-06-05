package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * The <code>AggFunctionQueryContext</code> class contains extracted details from QueryContext that can be used for
 * Aggregation Functions.
 */
public class AggFunctionQueryContext {
  private boolean _isNullHandlingEnabled;
  private List<OrderByExpressionContext> _orderByExpressions = null;
  private int _limit;

  /**
   * Extracts necessary fields from QueryContext
   */
  public AggFunctionQueryContext(QueryContext queryContext) {
    _isNullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _orderByExpressions = queryContext.getOrderByExpressions();
    _limit = queryContext.getLimit();
  }

  /**
   * Called from Multistage AggregateOperator to set the necessary context.
   */
  public AggFunctionQueryContext(boolean isNullHandlingEnabled) {
    _isNullHandlingEnabled = isNullHandlingEnabled;
  }

  public boolean isNullHandlingEnabled() {
    return _isNullHandlingEnabled;
  }

  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  public int getLimit() {
    return _limit;
  }
}

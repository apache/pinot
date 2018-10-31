package com.linkedin.thirdeye.dataframe.util;

import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import java.util.List;


/**
 * Wrapper for ThirdEye request with derived metric expressions
 */
public class RequestContainer {
  final ThirdEyeRequest request;
  final List<MetricExpression> expressions;

  RequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions) {
    this.request = request;
    this.expressions = expressions;
  }

  public ThirdEyeRequest getRequest() {
    return request;
  }

  public List<MetricExpression> getExpressions() {
    return expressions;
  }
}

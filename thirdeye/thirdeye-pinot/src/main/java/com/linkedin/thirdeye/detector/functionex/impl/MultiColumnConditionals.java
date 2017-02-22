package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.dataframe.DoubleSeries;
import com.linkedin.thirdeye.detector.functionex.dataframe.Series;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang3.math.NumberUtils;

public class MultiColumnConditionals extends AnomalyFunctionEx {

  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    String conditions = getConfig("conditions");
    String datasource = getConfig("datasource");
    String query = getConfig("query");

    DataFrame df = queryDataSource(datasource, query);

    AnomalyFunctionExResult result = new AnomalyFunctionExResult();

    Collection<String> allConditions = Arrays.asList(conditions.replace(" ", "").split(","));
    for(String condition : allConditions) {
      String[] fragments = condition.split(">=|<=|==|!=|>|<", 2);
      String column = fragments[0];
      String value = fragments[1];
      String operator = condition.substring(column.length(), condition.length() - value.length());

      OperatorType op = OperatorType.getOperator(operator);

      Series series = df.get(column);

      String message;
      boolean condition_pass = true;
      if(NumberUtils.isNumber(value)) {
        double d = Double.valueOf(value);
        condition_pass = series.toDoubles().map(new CustomDoubleConditional(op, d)).allTrue();
        message = String.format("%s: %b", condition, condition_pass);
      } else {
        // TODO implement boolean
        // TODO implement String
        throw new IllegalArgumentException(String.format("Value '%s' is not a number", value));
      }

      if(!condition_pass) {
        result.addAnomaly(getContext().getMonitoringWindowStart(), getContext().getMonitoringWindowEnd(), message);
      }
    }

    return result;
  }

  enum OperatorType {
    GT(">"),
    GTE(">="),
    LT("<"),
    LTE("<="),
    EQ("=="),
    NEQ("!=");

    final String text;
    OperatorType(String text) {
      this.text = text;
    }

    // workaround for reserved tokens
    // TODO hash map please
    static OperatorType getOperator(String operator) {
      for(OperatorType op : OperatorType.values()) {
        if(op.text.equals(operator))
          return op;
      }
      throw new IllegalArgumentException(String.format("Unknown operator '%s'", operator));
    }

  }

  static class CustomDoubleConditional implements DoubleSeries.DoubleConditional {
    final OperatorType operator;
    final double threshold;

    public CustomDoubleConditional(OperatorType operator, double threshold) {
      this.operator = operator;
      this.threshold = threshold;
    }

    @Override
    public boolean apply(double value) {
      switch(this.operator) {
        case GT:
          return value > threshold;
        case GTE:
          return value >= threshold;
        case LT:
          return value < threshold;
        case LTE:
          return value <= threshold;
        case EQ:
          return value == threshold;
        case NEQ:
          return value != threshold;
        default:
          throw new IllegalArgumentException(String.format("Operator %s does not apply to DoubleSeries", this.operator));
      }
    }
  }
}

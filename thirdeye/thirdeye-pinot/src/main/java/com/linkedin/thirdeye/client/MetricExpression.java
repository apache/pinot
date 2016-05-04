package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import parsii.eval.Expression;
import parsii.eval.Parser;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.tokenizer.ParseException;

public class MetricExpression {

  private String espressionName;
  private String expression;

  public MetricExpression() {

  }

  public MetricExpression(String expression) {
    this(expression.replaceAll("[\\s]+", ""), expression);
  }

  public MetricExpression(String espressionName, String expression) {
    this.espressionName = espressionName;
    this.expression = expression;
  }

  public String getEspressionName() {
    return espressionName;
  }

  public void setEspressionName(String espressionName) {
    this.espressionName = espressionName;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public String toString() {
    return expression;
  }

  public List<MetricFunction> computeMetricFunctions() {
    try {
      Scope scope = Scope.create();
      Set<String> metricNames = new TreeSet<>();

      // FIXME: __COUNT parse error
      if (expression.equals("__COUNT")) {
        metricNames.add("__COUNT");
      } else {
        Parser.parse(expression, scope);
        metricNames = scope.getLocalNames();
      }

      ArrayList<MetricFunction> metricFunctions = new ArrayList<>();
      for (String metricName : metricNames) {
        metricFunctions.add(MetricFunction.from(MetricFunction.SUM, metricName));
      }
      return metricFunctions;
    } catch (ParseException e) {
      throw new RuntimeException("Exception parsing expressionString:" + expression, e);
    }
  }

  public static double evaluateExpression(String expressionString,
      Map<String, Double> metricValueContext) throws Exception {
    Scope scope = Scope.create();
    Expression expression = Parser.parse(expressionString, scope);
    for (String metricName : metricValueContext.keySet()) {
      Variable variable = scope.create(metricName);
      if (!metricValueContext.containsKey(metricName)) {
        throw new Exception("No value set for metric:" + metricName + "  in the context:"
            + metricValueContext);
      }
      variable.setValue(metricValueContext.get(metricName));
    }
    return expression.evaluate();
  }

  public static void main(String[] args) throws ParseException {
    Scope s = Scope.create();
    Expression expression = Parser.parse("sqrt(a)/b", s);
    System.out.println(s.getLocalNames());
    Variable a = s.create("a");
    Variable b = s.create("b");
    a.setValue(10);
    b.setValue(5);

    System.out.println(expression.evaluate());

  }
}

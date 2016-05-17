package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.HashMap;
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

  private static String COUNT_METRIC = "__COUNT";
  private static String COUNT_METRIC_ESCAPED = "A__COUNT";

  private String expressionName;
  private String expression;

  public MetricExpression() {

  }

  public MetricExpression(String expression) {
    this(expression.replaceAll("[\\s]+", ""), expression);
  }

  public MetricExpression(String espressionName, String expression) {
    this.expressionName = espressionName;
    this.expression = expression;
  }

  public String getEspressionName() {
    return expressionName;
  }

  public void setEspressionName(String espressionName) {
    this.expressionName = espressionName;
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

      // expression parser errors out on variables starting with _
      // we're replacing the __COUNT default metric, with an escaped string
      // after evaluatin, we replace the escaped string back with the original
      String modifiedExpressions = expression.replace(COUNT_METRIC, COUNT_METRIC_ESCAPED);

      Parser.parse(modifiedExpressions, scope);
      metricNames = scope.getLocalNames();

      ArrayList<MetricFunction> metricFunctions = new ArrayList<>();
      for (String metricName : metricNames) {
        if(metricName.equals(COUNT_METRIC_ESCAPED)){
          metricName = COUNT_METRIC;
        }
        metricFunctions.add(MetricFunction.from(MetricFunction.SUM, metricName));
      }
      return metricFunctions;
    } catch (ParseException e) {
      throw new RuntimeException("Exception parsing expressionString:" + expression, e);
    }
  }

  public static double evaluateExpression(MetricExpression expression, Map<String, Double> context)
      throws Exception {
    return evaluateExpression(expression.getExpression(), context);
  }

  public static double evaluateExpression(String expressionString, Map<String, Double> context)
      throws Exception {

    Scope scope = Scope.create();
    expressionString = expressionString.replace(COUNT_METRIC, COUNT_METRIC_ESCAPED);
    Map<String, Double> metricValueContext = context;
    if (context.containsKey(COUNT_METRIC)) {
      metricValueContext = new HashMap<>(context);
      metricValueContext.put(COUNT_METRIC_ESCAPED, context.get(COUNT_METRIC));
    }
    Expression expression = Parser.parse(expressionString, scope);
    for (String metricName : metricValueContext.keySet()) {
      Variable variable = scope.create(metricName);
      if (!metricValueContext.containsKey(metricName)) {
        throw new Exception(
            "No value set for metric:" + metricName + "  in the context:" + metricValueContext);
      }
      variable.setValue(metricValueContext.get(metricName));
    }
    return expression.evaluate();
  }

  public static void main(String[] args) throws Exception {
    String expressionString = "(successCount)/(__COUNT)";
    MetricExpression expression = new MetricExpression("Approval_Rate", expressionString);
    Map<String, Double> metricValueContext = new HashMap<>();
    metricValueContext.put("__COUNT", 10d);
    metricValueContext.put("successCount", 10d);
    double result = MetricExpression.evaluateExpression(expressionString, metricValueContext);
    System.out.println(result);

  }
}

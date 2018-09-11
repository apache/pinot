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

package com.linkedin.thirdeye.datasource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import parsii.eval.Expression;
import parsii.eval.Parser;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.tokenizer.ParseException;

/**
 * This class maintains the metric name, the metric expression composed of metric ids, and the aggregation function
 * The dataset is required here, because we need to know which dataset to query in cases of count(*)  and select max(time)
 * For other cases, it can be derived from the metric id in the expression
 */
public class MetricExpression {

  private static String COUNT_METRIC = "__COUNT";
  private static String COUNT_METRIC_ESCAPED = "A__COUNT";

  private String expressionName;
  private String expression;
  private MetricAggFunction aggFunction = MetricAggFunction.SUM;
  private String dataset;

  public MetricExpression(String expression, String dataset) {
    this(expression.replaceAll("[\\s]+", ""), expression, dataset);
  }

  public MetricExpression(String expressionName, String expression, String dataset) {
    this(expressionName, expression, MetricAggFunction.SUM, dataset);
  }

  public MetricExpression(String expressionName, String expression, MetricAggFunction aggFunction, String dataset) {
    this.expressionName = expressionName;
    this.expression = expression;
    this.aggFunction = aggFunction;
    this.dataset = dataset;
  }

  public String getExpressionName() {
    return expressionName;
  }


  public String getExpression() {
    return expression;
  }


  public String getDataset() {
    return dataset;
  }


  @Override
  public String toString() {
    return expression;
  }

  public List<MetricFunction> computeMetricFunctions() {
    try {
      Scope scope = Scope.create();
      Set<String> metricTokens = new TreeSet<>(); // can be either metric names or ids ! :-/

      // expression parser errors out on variables starting with _
      // we're replacing the __COUNT default metric, with an escaped string
      // after evaluating, we replace the escaped string back with the original
      String modifiedExpressions = expression.replace(COUNT_METRIC, COUNT_METRIC_ESCAPED);

      Parser.parse(modifiedExpressions, scope);
      metricTokens = scope.getLocalNames();

      ArrayList<MetricFunction> metricFunctions = new ArrayList<>();
      for (String metricToken : metricTokens) {
        Long metricId = null;
        MetricConfigDTO metricConfig = null;
        String metricDataset = dataset;
        DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(metricDataset);
        if (metricToken.equals(COUNT_METRIC_ESCAPED)) {
          metricToken = COUNT_METRIC;
        } else {
          metricId = Long.valueOf(metricToken.replace(MetricConfigBean.DERIVED_METRIC_ID_PREFIX, ""));
          metricConfig = ThirdEyeUtils.getMetricConfigFromId(metricId);
          if (metricConfig != null) {
            metricDataset = metricConfig.getDataset();
          }
        }
        metricFunctions.add(
            new MetricFunction(aggFunction, metricToken, metricId, metricDataset, metricConfig, datasetConfig));
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
    metricValueContext.put("__COUNT", 0d);
    metricValueContext.put("successCount", 0d);
    double result = MetricExpression.evaluateExpression(expressionString, metricValueContext);
    System.out.println(Double.isInfinite(result));

  }
}

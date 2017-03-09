package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.dataframe.DoubleSeries;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * CONFIG:
 * variables: {
 *   var_a: "metric://pinot/dataset/metric/function?key=value",
 *   var_b: "metric://pinot/dataset/metric/function?key=other_value"
 * },
 * rules: {
 *   rule_1: "var_a >= var_b",
 *   rule_2: "var_a > 10",
 *   rule_3: "var_b > 10"
 * }
 */

public class RulesFunction extends AnomalyFunctionEx {
  private static final Logger LOG = LoggerFactory.getLogger(RulesFunction.class);

  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    Map<String, String> variables = getSubConfig(getContext().getConfig(), "variables.");
    Map<String, String> rules = getSubConfig(getContext().getConfig(), "rules.");

    LOG.info("Using {} variables: {}", variables.size(), variables.keySet());
    LOG.info("Using {} rules: {}", rules.size(), rules.keySet());

    Map<String, DataFrame> dataFrames = new HashMap<>();

    LOG.info("Populating variables:");
    for(Map.Entry<String, String> e : variables.entrySet()) {
      URI uri = URI.create(e.getValue());
      LOG.info("Fetching '{}': '{}'", e.getKey(), uri);
      DataFrame queryResult = queryDataSource(uri.getScheme(), uri.toString());
      dataFrames.put(e.getKey(), queryResult.sortBySeries("timestamp"));
    }

    DataFrame data = mergeDataFrames(dataFrames);

    AnomalyFunctionExResult anomalyResult = new AnomalyFunctionExResult();
    anomalyResult.setContext(getContext());

    LOG.info("Applying rules:");
    for(Map.Entry<String, String> e : rules.entrySet()) {
      LOG.info("Applying '{}': '{}'", e.getKey(), e.getValue());
      DoubleSeries ruleResults = data.map(e.getValue());
      DataFrame violations = data.filter(ruleResults.toBooleans().not());
      LOG.info("Rule '{}' violated at {} out of {} timestamps in monitoring window", e.getKey(), violations.size(), ruleResults.size());

      long[] timestamps = violations.toLongs("timestamp").values();
      for(int i=0; i<timestamps.length; i++) {
        anomalyResult.addAnomaly(timestamps[i], timestamps[i], String.format("Rule '%s' violated", e.getKey()));
      }
    }

    return anomalyResult;
  }

  static DataFrame mergeDataFrames(Map<String, DataFrame> dataFrames) {
    // TODO: move to data frame, check indices and timestamps

    if(dataFrames.isEmpty())
      return new DataFrame(0);

    DataFrame first = dataFrames.values().iterator().next();
    DataFrame df = new DataFrame(first.getIndex());
    df.addSeries("timestamp", first.get("timestamp"));

    for(Map.Entry<String, DataFrame> e : dataFrames.entrySet()) {
      df.addSeries(e.getKey(), e.getValue().get("metric"));
    }

    return df;
  }

  static Map<String, String> getSubConfig(Map<String, String> map, String prefix) {
    Map<String, String> output = new HashMap<>();
    for(Map.Entry<String, String> e : map.entrySet()) {
      if(e.getKey().startsWith(prefix)) {
        String subKey = e.getKey().substring(prefix.length());
        output.put(subKey, e.getValue());
      }
    }
    return output;
  }
}

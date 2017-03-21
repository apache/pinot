package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesFunction extends AnomalyFunctionEx {
  private static final Logger LOG = LoggerFactory.getLogger(RulesFunction.class);

  private static final String COLUMN_TIMESTAMP = "timestamp";
  private static final String COLUMN_METRIC = "metric";

  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    Map<String, String> variables = getSubConfig(getContext().getConfig(), "variables.");
    Map<String, String> rules = getSubConfig(getContext().getConfig(), "rules.");

    LOG.info("Using {} variables: {}", variables.size(), variables.keySet());
    LOG.info("Using {} rules: {}", rules.size(), rules.keySet());

    Map<String, DataFrame> dataFrames = new HashMap<>();

    LOG.info("Populating variables:");
    for (Map.Entry<String, String> e : variables.entrySet()) {
      URI uri = URI.create(e.getValue());
      LOG.info("Fetching '{}': '{}'", e.getKey(), uri);
      DataFrame queryResult = queryDataSource(uri.getScheme(), uri.toString());
      dataFrames.put(e.getKey(), queryResult);
    }

    DataFrame data = mergeDataFrames(dataFrames);

    LOG.info("{}", data);

    long timestepSize = estimateStepSize(data);

    AnomalyFunctionExResult anomalyResult = new AnomalyFunctionExResult();
    anomalyResult.setContext(getContext());

    LOG.info("Applying rules:");
    for (Map.Entry<String, String> e : rules.entrySet()) {
      String rule = e.getValue();
      DoubleSeries ruleResults = data.map(rule);
      DataFrame violations = data.filter(ruleResults.getBooleans());
      LOG.info("Rule '{}' violated at {} / {} timestamps", e.getKey(), violations.size(), ruleResults.size());

      long[] timestamps = violations.getLongs(COLUMN_TIMESTAMP).values();
      for (int i = 0; i < timestamps.length; i++) {
        DataFrame slice = violations.slice(i, i+1);

        String numeric = extractNumericPortion(rule);
        String threshold = extractThresholdPortion(rule);

        DataFrame debug = new DataFrame(1);
        debug.addSeries("current", slice.map(numeric).first());
        debug.addSeries("baseline", slice.map(threshold).first());

        anomalyResult.addAnomaly(timestamps[i] - timestepSize, timestamps[i], String.format("Rule '%s' violated", e.getKey()), debug);
      }
    }

    return anomalyResult;
  }

  static DataFrame mergeDataFrames(Map<String, DataFrame> dataFrames) {
    if(dataFrames.isEmpty())
      return new DataFrame(0);

    Iterator<Map.Entry<String, DataFrame>> it = dataFrames.entrySet().iterator();

    Map.Entry<String, DataFrame> entry = it.next();
    entry.getValue().renameSeries(COLUMN_METRIC, entry.getKey());
    DataFrame data = entry.getValue();

    while(it.hasNext()) {
      Map.Entry<String, DataFrame> e = it.next();
      e.getValue().renameSeries(COLUMN_METRIC, e.getKey());
      data = data.joinInner(e.getValue(), COLUMN_TIMESTAMP, COLUMN_TIMESTAMP);
    }

    return data.dropNull();
  }

  static Map<String, String> getSubConfig(Map<String, String> map, String prefix) {
    Map<String, String> output = new TreeMap<>();
    for (Map.Entry<String, String> e : map.entrySet()) {
      if (e.getKey().startsWith(prefix)) {
        String subKey = e.getKey().substring(prefix.length());
        output.put(subKey, e.getValue());
      }
    }
    return output;
  }

  static long estimateStepSize(DataFrame df) {
    if(df.size() <= 1)
      return 0;

    Series ts = df.getLongs(COLUMN_TIMESTAMP);

    return DataFrame.map(new Series.LongFunction() {
      @Override
      public long apply(long[] values) {
        return values[0] - values[1];
      }
    }, ts, ts.shift(-1)).min();
  }

  static String extractNumericPortion(String rule) {
    Pattern p = Pattern.compile(">=|<=|>|<|!=|==|=");
    Matcher m = p.matcher(rule);
    if(!m.find())
      return "0";
    return rule.substring(0, m.start()).trim();
  }

  static String extractThresholdPortion(String rule) {
    Pattern p = Pattern.compile(">=|<=|>|<|!=|==|=");
    Matcher m = p.matcher(rule);
    if(!m.find())
      return "0";
    return rule.substring(m.end()).trim();
  }
}

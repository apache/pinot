package com.linkedin.thirdeye.anomaly.views;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CondensedAnomalyTimelinesView {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  Long bucketMillis = 0l;
  List<Long> timeStamps = new ArrayList<>();
  Map<String, String> summary = new HashMap<>();
  List<Double> currentValues = new ArrayList<>();
  List<Double> baselineValues = new ArrayList<>();

  public Long getBucketMillis() {
    return bucketMillis;
  }

  public void setBucketMillis(Long bucketMillis) {
    this.bucketMillis = bucketMillis;
  }

  public List<Long> getTimeStamps() {
    return timeStamps;
  }

  public void addTimeStamps(Long timeStamp) {
    this.timeStamps.add(timeStamp);
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void addSummary(String key, String value) {
    this.summary.put(key, value);
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void addCurrentValues(Double currentValue) {
    this.currentValues.add(currentValue);
  }

  public List<Double> getBaselineValues() {
    return baselineValues;
  }

  public void addBaselineValues(Double baselineValue) {
    this.baselineValues.add(baselineValue);
  }

  public AnomalyTimelinesView toAnomalyTimelinesView() {
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();
    for (int i = 0; i < timeStamps.size(); i++) {
      TimeBucket timeBucket = new TimeBucket(timeStamps.get(i), timeStamps.get(i) + bucketMillis,
          timeStamps.get(i), timeStamps.get(i) + bucketMillis);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      anomalyTimelinesView.addBaselineValues(baselineValues.get(i));
      anomalyTimelinesView.addCurrentValues(currentValues.get(i));
    }
    for (Map.Entry<String, String> entry : summary.entrySet()) {
      anomalyTimelinesView.addSummary(entry.getKey(), entry.getValue());
    }
    return anomalyTimelinesView;
  }

  public static CondensedAnomalyTimelinesView fromAnomalyTimelinesView(AnomalyTimelinesView anomalyTimelinesView) {
    CondensedAnomalyTimelinesView condensedView = new CondensedAnomalyTimelinesView();
    for (int i = 0; i < anomalyTimelinesView.getTimeBuckets().size(); i++) {
      TimeBucket timeBucket = anomalyTimelinesView.getTimeBuckets().get(i);
      condensedView.setBucketMillis(timeBucket.getCurrentEnd() - timeBucket.getCurrentStart());
      condensedView.addTimeStamps(timeBucket.getCurrentStart());
      condensedView.addCurrentValues(anomalyTimelinesView.getCurrentValues().get(i));
      condensedView.addBaselineValues(anomalyTimelinesView.getBaselineValues().get(i));
    }

    for (Map.Entry<String, String> entry : anomalyTimelinesView.getSummary().entrySet()) {
      condensedView.addSummary(entry.getKey(), entry.getValue());
    }
    return condensedView;
  }

  /**
   * Convert current instance into JSON String using ObjectMapper
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @return
   *    The JSON String of current instance
   * @throws JsonProcessingException
   */
  public String toJsonString() throws JsonProcessingException {
    String jsonString = OBJECT_MAPPER.writeValueAsString(this);

    return jsonString;
  }

  /**
   * Given the JSON String of an AnomalyTimelinesView, return an instance of AnomalyTimelinesView
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @param jsonString
   *    The JSON String of an instance
   * @return
   *    An instance based on the given JSON String
   * @throws IOException
   */
  public static CondensedAnomalyTimelinesView fromJsonString(String jsonString) throws IOException {
    CondensedAnomalyTimelinesView view = OBJECT_MAPPER.readValue(jsonString, CondensedAnomalyTimelinesView.class);
    return view;
  }
}

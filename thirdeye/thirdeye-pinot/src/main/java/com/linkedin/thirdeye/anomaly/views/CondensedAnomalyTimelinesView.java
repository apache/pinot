package com.linkedin.thirdeye.anomaly.views;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * We face a problem that if we store the timelines view using AnomalyTimelinesView. The DB has overflow exception when
 * storing merged anomalies. As TimeBucket is not space efficient for storage. To solve the problem, we introduce a
 * condensed version of AnomalyTimelinesView.
 */
public class CondensedAnomalyTimelinesView {
  public static final int DEFAULT_MAX_LENGTH = 1024 * 10; // 10 kilobytes
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final Long DEFAULT_MIN_BUCKET_UNIT = 1000l;

  Long bucketMillis = 0l;  // the bucket size of original time series
  List<Long> timeStamps = new ArrayList<>(); // the start time of data points
  Map<String, String> summary = new HashMap<>(); // the summary of the view
  List<Double> currentValues = new ArrayList<>();  // the observed values
  List<Double> baselineValues = new ArrayList<>(); // the expected values

  public CondensedAnomalyTimelinesView() {

  }

  public CondensedAnomalyTimelinesView(Long bucketMillis, List<Long> timeStamps, List<Double> currentValues,
      List<Double> baselineValues, Map<String, String> summary) {
    this.bucketMillis = bucketMillis / DEFAULT_MIN_BUCKET_UNIT;
    for (long timestamp : timeStamps) {
      this.timeStamps.add(timestamp / DEFAULT_MIN_BUCKET_UNIT);
    }
    this.summary = summary;
    this.currentValues = currentValues;
    this.baselineValues = baselineValues;
  }

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

  /**
   * Convert current instance to an AnomalyTimelinesView instance
   * @return
   */
  public AnomalyTimelinesView toAnomalyTimelinesView() {
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();
    for (int i = 0; i < timeStamps.size(); i++) {
      TimeBucket timeBucket = new TimeBucket(timeStamps.get(i) * DEFAULT_MIN_BUCKET_UNIT,
          (timeStamps.get(i) + bucketMillis) * DEFAULT_MIN_BUCKET_UNIT,
          timeStamps.get(i) * DEFAULT_MIN_BUCKET_UNIT,
          (timeStamps.get(i) + bucketMillis) * DEFAULT_MIN_BUCKET_UNIT);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      anomalyTimelinesView.addBaselineValues(baselineValues.get(i));
      anomalyTimelinesView.addCurrentValues(currentValues.get(i));
    }
    for (Map.Entry<String, String> entry : summary.entrySet()) {
      anomalyTimelinesView.addSummary(entry.getKey(), entry.getValue());
    }
    return anomalyTimelinesView;
  }

  /**
   * Convert an AnomalyTimelinesView instance to CondensedAnomalyTimelinesView
   * @param anomalyTimelinesView
   * @return
   */
  public static CondensedAnomalyTimelinesView fromAnomalyTimelinesView(AnomalyTimelinesView anomalyTimelinesView) {
    CondensedAnomalyTimelinesView condensedView = new CondensedAnomalyTimelinesView();
    long maxBucketMillis = 0;
    for (int i = 0; i < anomalyTimelinesView.getTimeBuckets().size(); i++) {
      TimeBucket timeBucket = anomalyTimelinesView.getTimeBuckets().get(i);
      maxBucketMillis = Math.max(maxBucketMillis, timeBucket.getCurrentEnd() - timeBucket.getCurrentStart()) / DEFAULT_MIN_BUCKET_UNIT;
      condensedView.addTimeStamps(timeBucket.getCurrentStart() / DEFAULT_MIN_BUCKET_UNIT);
      condensedView.addCurrentValues(anomalyTimelinesView.getCurrentValues().get(i));
      condensedView.addBaselineValues(anomalyTimelinesView.getBaselineValues().get(i));
    }
    condensedView.setBucketMillis(maxBucketMillis);

    for (Map.Entry<String, String> entry : anomalyTimelinesView.getSummary().entrySet()) {
      condensedView.addSummary(entry.getKey(), entry.getValue());
    }
    return condensedView;
  }

  public CondensedAnomalyTimelinesView compress() {
    return compress(DEFAULT_MAX_LENGTH);
  }

  public CondensedAnomalyTimelinesView compress(int maxLength) {
    if (timeStamps.size() == 0) {
      return this;
    }
    try {
      if (this.toJsonString().length() < maxLength) {
        return this;
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to parse view to json string", e);
    }

    List<Long> aggregatedTimestamps = new ArrayList<>();
    List<Double> aggregatedObservedValues = new ArrayList<>();
    List<Double> aggregatedExpectedValues = new ArrayList<>();
    long maxBucketMills = this.bucketMillis * 2;
    long lastTimestampEnd = this.timeStamps.get(this.timeStamps.size() - 1) + bucketMillis;

    for (int i = 0; i < timeStamps.size(); i++) {
      int count = 1;
      long timestamp = this.timeStamps.get(i);
      double observedValue = this.currentValues.get(i);
      double expectedValue = this.baselineValues.get(i);
      /*
       Aggregate data points fit in the bucket window; remain the same if it cannot fit
       */
      if ((lastTimestampEnd - timestamp) >= maxBucketMills) {
        while (i + 1 < this.timeStamps.size() && (this.timeStamps.get(i + 1) - timestamp) < maxBucketMills) {
          observedValue += this.currentValues.get(i + 1);
          expectedValue += this.baselineValues.get(i + 1);
          i++;
          count++;
        }
      }
      aggregatedTimestamps.add(timestamp * DEFAULT_MIN_BUCKET_UNIT);
      aggregatedObservedValues.add(observedValue/((double)count));
      aggregatedExpectedValues.add(expectedValue/((double)count));
    }


    return new CondensedAnomalyTimelinesView(maxBucketMills * DEFAULT_MIN_BUCKET_UNIT, aggregatedTimestamps, aggregatedObservedValues,
        aggregatedExpectedValues, new HashMap<String, String>(summary)).compress(maxLength);
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

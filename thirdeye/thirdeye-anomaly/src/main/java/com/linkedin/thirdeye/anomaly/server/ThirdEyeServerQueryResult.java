package com.linkedin.thirdeye.anomaly.server;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDataset;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class ThirdEyeServerQueryResult extends AnomalyDetectionDataset {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeServerQueryResult.class);

  /**
   * @param responseBody
   */
  public ThirdEyeServerQueryResult(List<MetricSpec> metricSpecs, String responseBody) {
    try {
      JSONObject json = new JSONObject(responseBody);
      dimensions = jsonArrayToList(json.getJSONArray("dimensions"));
      metrics = jsonArrayToList(json.getJSONArray("metrics"));
      jsonLoadData(metricSpecs, json.getJSONObject("data"));
    } catch (JSONException e) {
      LOGGER.error("could not parse response from ThirdEye server", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void jsonLoadData(List<MetricSpec> metricsSpecs, JSONObject jsonObject) throws JSONException {
    Iterator<String> it = jsonObject.keys();
    while (it.hasNext()) {
      String key = it.next();
      DimensionKey dimensionKey = getDimensionKey(new JSONArray(key));
      MetricTimeSeries timeSeries = getMetricTimeSeries(metricsSpecs, jsonObject.getJSONObject(key));
      data.put(dimensionKey, timeSeries);
    }
  }

  @SuppressWarnings("unchecked")
  private MetricTimeSeries getMetricTimeSeries(List<MetricSpec> metricSpec, JSONObject jsonObject)
      throws JSONException {
    MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(metricSpec));

    Iterator<String> it = jsonObject.keys();
    while (it.hasNext()) {
      String datetimeMillisString = it.next();
      long timeWindow = Long.valueOf(datetimeMillisString);

      JSONArray valueArray = jsonObject.getJSONArray(datetimeMillisString);
      for (int i = 0; i < valueArray.length(); i++) {
        Number value = valueArray.getDouble(i);
        String name = metrics.get(i);
        timeSeries.set(timeWindow, name, value);
      }
    }

    LOGGER.debug("query returned time series of size : {}", timeSeries.getTimeWindowSet().size());
    return timeSeries;
  }

  private DimensionKey getDimensionKey(JSONArray jsonArray) throws JSONException {
    String[] dimensionValues = new String[jsonArray.length()];
    for (int i = 0; i < jsonArray.length(); i++) {
      dimensionValues[i] = jsonArray.getString(i);
    }
    return new DimensionKey(dimensionValues);
  }

  private List<String> jsonArrayToList(JSONArray jsonArray) throws JSONException {
    List<String> ret = new ArrayList<>(jsonArray.length());
    for (int i = 0; i < jsonArray.length(); i++) {
      ret.add(jsonArray.getString(i));
    }
    return ret;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.views.heatmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.pinot.thirdeye.dashboard.views.GenericResponse;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse.Info;
import org.apache.pinot.thirdeye.dashboard.views.ViewResponse;

/**
 * Header: dimension,
 * Schema: dimensionValue baseline current
 */
public class HeatMapViewResponse implements ViewResponse {

  List<String> metrics;
  List<String> dimensions;
  Info summary;
  Map<String, GenericResponse> data;
  Map<String, String> metricExpression;
  boolean inverseMetric = false;

  public HeatMapViewResponse() {
    super();
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public Info getSummary() {
    return summary;
  }

  public void setSummary(Info summary) {
    this.summary = summary;
  }

  public Map<String, GenericResponse> getData() {
    return data;
  }

  public void setData(Map<String, GenericResponse> data) {
    this.data = data;
  }


  public Map<String, String> getMetricExpression() {
    return metricExpression;
  }

  public void setMetricExpression(Map<String, String> metricExpression) {
    this.metricExpression = metricExpression;
  }


  public boolean isInverseMetric() {
    return inverseMetric;
  }

  public void setInverseMetric(boolean inverseMetric) {
    this.inverseMetric = inverseMetric;
  }

  private static Map<String, String> generateSummary(GenericResponse genericResponse) {
    Map<String, String> summary = new HashMap<>();

    Map<String, Integer> columnsToIndexMapping =
        genericResponse.getSchema().getColumnsToIndexMapping();
    Double totalBaselineValue = 0d;
    Double totalCurrentValue = 0d;
    Map<String, String> simpleFields = genericResponse.getSummary().getSimpleFields();
    for (String[] rowData : genericResponse.getResponseData()) {
      int i = columnsToIndexMapping.get("baselineValue");
      String baselineValue = rowData[i];
      totalBaselineValue = totalBaselineValue + Double.valueOf(baselineValue);

      int j = columnsToIndexMapping.get("currentValue");
      String currentValue = rowData[j];
      totalCurrentValue = totalCurrentValue + Double.valueOf(currentValue);
    }
    summary.put("totalBaselineValue", String.valueOf(totalBaselineValue));
    summary.put("totalBaselineValue", String.valueOf(totalBaselineValue));

    summary.putAll(simpleFields);

    return summary;
  }

  public void updateStats() {

    for (Map.Entry<String, GenericResponse> entry : data.entrySet()) {
      GenericResponse genericResponse = entry.getValue();

      Map<String, Integer> columnsToIndexMapping =
          genericResponse.getSchema().getColumnsToIndexMapping();
      int i = columnsToIndexMapping.get("baselineValue");
      int j = columnsToIndexMapping.get("currentValue");

      for (String[] rowData : genericResponse.getResponseData()) {
        Double baselineValue = Double.valueOf(rowData[i]);
        Double currentValue = Double.valueOf(rowData[j]);

        DescriptiveStatistics baselineStats = new DescriptiveStatistics();
        DescriptiveStatistics currentStats = new DescriptiveStatistics();

        // Add stats
        // Update columns
        // Update column to index mapping
      }
    }
  }

  public static void main(String[] args) throws Exception {
    generateOverviewJSON();
  }

  private static void generateContributorJSON() throws JSONException {
    JSONObject json = new JSONObject();
    int numTimeBuckets = 24;
    int numMetrics = 1;
    int numDimensions = 2;
    JSONObject metricNames = new JSONObject();
    JSONObject timeBuckets = new JSONObject();
    JSONArray metricArrayJson = new JSONArray();
    for (int m = 0; m < numMetrics; m++) {
      JSONObject metricJson = new JSONObject();
      metricJson.put("metricName", "M" + m);
      JSONArray dimensionArray = new JSONArray();
      for (int d = 0; d < numDimensions; d++) {
        JSONObject dimensionJson = new JSONObject();
        dimensionJson.put("dimensionName", "D" + d);
        JSONArray dataArrayJson = new JSONArray();
        for (int t = 0; t < numTimeBuckets; t++) {
          JSONArray entry = new JSONArray();
          entry.put(100);
          entry.put(90);
          dataArrayJson.put(entry);
        }
        dimensionJson.put("data", dataArrayJson);
      }
      metricJson.put("dimensionData", dimensionArray);
      metricArrayJson.put(metricJson);
    }
    json.put("metricNames", metricNames);
    json.put("timeBuckets", timeBuckets);
    json.put("metricDataArray", metricArrayJson);
    System.out.println(json.toString(1));
  }

  private static void generateOverviewJSON() throws JSONException {
    JSONObject json = new JSONObject();
    int numTimeBuckets = 24;
    int numMetrics = 10;
    JSONObject metricNames = new JSONObject();
    JSONObject timeBuckets = new JSONObject();
    JSONArray metricArrayJson = new JSONArray();
    for (int m = 0; m < numMetrics; m++) {
      JSONObject metricJson = new JSONObject();
      metricJson.put("metricName", "M" + m);
      JSONArray timeArrayJson = new JSONArray();
      for (int t = 0; t < numTimeBuckets; t++) {
        JSONArray entry = new JSONArray();
        entry.put(100);
        entry.put(90);
        timeArrayJson.put(entry);
      }
      metricJson.put("data", timeArrayJson);
      metricArrayJson.put(metricJson);
    }
    json.put("metricNames", metricNames);
    json.put("timeBuckets", timeBuckets);
    json.put("metricDataArray", metricArrayJson);
    System.out.println(json.toString(1));
  }

  private static void generateHeatMapJSON() throws JSONException {
    JSONObject json = new JSONObject();
    int numMetrics = 1;
    int numDimensions = 2;
    JSONArray metricArrayJson = new JSONArray();
    for (int m = 0; m < numMetrics; m++) {
      JSONObject metricJson = new JSONObject();
      metricJson.put("metricName", "M" + m);
      JSONArray dimensionArrayJson = new JSONArray();
      for (int d = 0; d < numDimensions; d++) {
        JSONObject dimensionJson = new JSONObject();
        dimensionJson.put("dimensionName", "D" + d);
        JSONObject schema = new JSONObject();
        schema.put("dimValue", 0);
        schema.put("currentValue", 1);
        schema.put("baselineValue", 2);
        dimensionJson.put("schema", schema);
        JSONArray dimensionData = new JSONArray();
        for (int i = 0; i < 10; i++) {
          JSONArray entry = new JSONArray();
          entry.put("d" + i);
          entry.put(100);
          entry.put(90);
          dimensionData.put(entry);
        }
        dimensionJson.put("data", dimensionData);
        dimensionArrayJson.put(dimensionJson);
      }
      metricJson.put("dimensionDataArray", dimensionArrayJson);
      metricArrayJson.put(metricJson);
    }
    json.put("metricDataArray", metricArrayJson);
    JSONObject summary = new JSONObject();
    summary.put("baselineRange", "2016-01-01 To 2016-01-07");
    summary.put("currentRange", "2016-01-08 To 2016-01-15");
    json.put("summary", summary);
    json.put("type", "object");
    System.out.println(json.toString(1));
  }
}

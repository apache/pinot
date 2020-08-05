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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.google.inject.Inject;
import io.dropwizard.views.View;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.dashboard.views.DashboardView;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesHandler;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRequest;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesResponse;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRow;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class DashboardResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(DashboardResource.class);
  private static final String DEFAULT_TIMEZONE_ID = "UTC";

  private final SummaryResource summaryResource;
  private final QueryCache queryCache;

  @Inject
  public DashboardResource(final SummaryResource summaryResource) {
    this.summaryResource = summaryResource;
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
  }

  @GET
  @Path("/")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new DashboardView();
  }

  @Path("summary")
  public SummaryResource getSummaryResource() {
    return summaryResource;
  }

  @GET
  @Path(value = "/data/timeseries")
  @Produces(MediaType.APPLICATION_JSON)
  public String getTimeSeriesData(@QueryParam("dataset") String collection,
      @QueryParam("filters") String filterJson,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("currentStart") Long start, @QueryParam("currentEnd") Long end,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("metrics") String metricsJson, @QueryParam("dimensions") String groupByDimensions)
      throws Exception {


    TimeSeriesRequest request = new TimeSeriesRequest();
    request.setCollectionName(collection);

    // See {@link #getDashboardData} for the reason that the start and end time are stored in a
    // DateTime object with data's timezone.
    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setStart(new DateTime(start, timeZoneForCollection));
    request.setEnd(new DateTime(end, timeZoneForCollection));

    if (groupByDimensions != null && !groupByDimensions.isEmpty()) {
      request.setGroupByDimensions(Arrays.asList(groupByDimensions.trim().split(",")));
    }
    if (filterJson != null && !filterJson.isEmpty()) {
      filterJson = URLDecoder.decode(filterJson, "UTF-8");
      request.setFilterSet(ThirdEyeUtils.convertToMultiMap(filterJson));
    }
    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricsJson, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);

    request.setAggregationTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    DatasetConfigDTO datasetConfig = CACHE_REGISTRY_INSTANCE.getDatasetConfigCache().get(collection);
    TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);

    if (!request.getAggregationTimeGranularity().getUnit().equals(TimeUnit.DAYS) ||
        !StringUtils.isBlank(timespec.getFormat())) {
      request.setEndDateInclusive(true);
    }

    TimeSeriesHandler handler = new TimeSeriesHandler(queryCache);
    String jsonResponse = "";
    try {
      TimeSeriesResponse response = handler.handle(request);
      JSONObject timeseriesMap = new JSONObject();
      JSONArray timeValueArray = new JSONArray();
      TreeSet<String> keys = new TreeSet<>();
      TreeSet<Long> times = new TreeSet<>();
      for (int i = 0; i < response.getNumRows(); i++) {
        TimeSeriesRow timeSeriesRow = response.getRow(i);
        times.add(timeSeriesRow.getStart());
      }
      for (Long time : times) {
        timeValueArray.put(time);
      }
      timeseriesMap.put("time", timeValueArray);
      for (int i = 0; i < response.getNumRows(); i++) {
        TimeSeriesRow timeSeriesRow = response.getRow(i);
        for (TimeSeriesMetric metricTimeSeries : timeSeriesRow.getMetrics()) {
          String key = metricTimeSeries.getMetricName();
          if (timeSeriesRow.getDimensionNames() != null
              && timeSeriesRow.getDimensionNames().size() > 0) {
            StringBuilder sb = new StringBuilder(key);
            for (int idx = 0; idx < timeSeriesRow.getDimensionNames().size(); ++idx) {
              sb.append("||").append(timeSeriesRow.getDimensionNames().get(idx));
              sb.append("|").append(timeSeriesRow.getDimensionValues().get(idx));
            }
            key = sb.toString();
          }
          JSONArray valueArray;
          if (!timeseriesMap.has(key)) {
            valueArray = new JSONArray();
            timeseriesMap.put(key, valueArray);
            keys.add(key);
          } else {
            valueArray = timeseriesMap.getJSONArray(key);
          }
          valueArray.put(metricTimeSeries.getValue());
        }
      }
      JSONObject summaryMap = new JSONObject();
      summaryMap.put("currentStart", start);
      summaryMap.put("currentEnd", end);
      JSONObject jsonResponseObject = new JSONObject();
      jsonResponseObject.put("timeSeriesData", timeseriesMap);
      jsonResponseObject.put("keys", new JSONArray(keys));
      jsonResponseObject.put("summary", summaryMap);
      jsonResponse = jsonResponseObject.toString();
    } catch (Exception e) {
      throw e;
    }
    LOG.info("Response:{}", jsonResponse);
    return jsonResponse;
  }

  @GET
  @Path(value = "/data")
  @Produces(MediaType.APPLICATION_JSON)
  public String getData(@QueryParam("type") String type) throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String response = null;

    String file = null;
    if (type.equals("dataset")) {
      List<String> collections = CACHE_REGISTRY_INSTANCE.getDatasetsCache().getDatasets();
      JSONArray array = new JSONArray(collections);
      response = array.toString();
      // file = "assets/data/getdataset.json";
    } else if (type.equals("metrics")) {
      file = "assets/data/getmetrics.json";
    } else if (type.equals("treemaps")) {
      file = "assets/data/gettreemaps.json";
    } else {
      throw new Exception("Invalid param!!");
    }
    if (response == null) {
      InputStream inputStream = classLoader.getResourceAsStream(file);

      // ClassLoader classLoader = getClass().getClassLoader();
      // InputStream inputStream = classLoader.getResourceAsStream("assets.data/getmetrics.json");

      try {
        response = IOUtils.toString(inputStream);
      } catch (IOException e) {
        response = e.toString();
      }
    }
    return response;
  }
}

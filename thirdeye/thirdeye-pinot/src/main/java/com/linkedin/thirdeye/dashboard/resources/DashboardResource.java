package com.linkedin.thirdeye.dashboard.resources;

import io.dropwizard.views.View;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.DashboardView;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewHandler;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewRequest;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewResponse;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/dashboard")
public class DashboardResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(DashboardResource.class);
  private static final String DEFAULT_TIMEZONE_ID = "UTC";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private QueryCache queryCache;
  private LoadingCache<String, Long> datasetMaxDataTimeCache;
  private LoadingCache<String, String> dimensionFiltersCache;

  private MetricConfigManager metricConfigDAO;

  public DashboardResource() {
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.datasetMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
    this.dimensionFiltersCache = CACHE_REGISTRY_INSTANCE.getDimensionFiltersCache();
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
  }

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new DashboardView();
  }


  @GET
  @Path(value = "/data/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getMetrics(@QueryParam("dataset") String collection) throws Exception {
    try {
      List<String> metrics = new ArrayList<>();
      List<MetricConfigDTO> metricConfigs = metricConfigDAO.findActiveByDataset(collection);
      for (MetricConfigDTO metricConfig : metricConfigs) {
        metrics.add(metricConfig.getName());
      }
      Collections.sort(metrics);
      return metrics;
    } catch (Exception e) {
      LOG.error("Error while fetching metrics", e);
      throw e;
    }
  }

  @GET
  @Path(value = "/data/dimensions")
  @Produces(MediaType.APPLICATION_JSON)
  public String getDimensions(@QueryParam("dataset") String collection) {
    String jsonDimensions = null;
    try {
      List<String> dimensions = Utils.getSortedDimensionNames(collection);
      jsonDimensions = OBJECT_MAPPER.writeValueAsString(dimensions);
    } catch (Exception e) {
      LOG.error("Error while fetching dimensions for collection: " + collection, e);
    }
    return jsonDimensions;
  }


  @GET
  @Path(value = "/data/info")
  @Produces(MediaType.APPLICATION_JSON)
  public String getMaxTime(@QueryParam("dataset") String collection) {
    String collectionInfo = null;
    try {

      HashMap<String, String> map = new HashMap<>();
      long maxDataTime = datasetMaxDataTimeCache.get(collection);
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY_INSTANCE.getDatasetConfigCache().get(collection);
      TimeSpec timespec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
      TimeGranularity dataGranularity = timespec.getDataGranularity();
      map.put("maxTime", "" + maxDataTime);
      map.put("dataGranularity", dataGranularity.getUnit().toString());

      List<MetricConfigDTO> metricConfigs = metricConfigDAO.findActiveByDataset(collection);
      List<String> inverseMetrics = new ArrayList<>();
      for (MetricConfigDTO metricConfig : metricConfigs) {
        if (metricConfig.isInverseMetric()) {
          inverseMetrics.add(metricConfig.getName());
        }
      }

      if (CollectionUtils.isNotEmpty(inverseMetrics)) {
        map.put("invertColorMetrics", Joiner.on(",").join(inverseMetrics));
      }
      collectionInfo = OBJECT_MAPPER.writeValueAsString(map);

    } catch (Exception e) {
      LOG.error("Error while fetching info for collection: " + collection, e);
    }
    return collectionInfo;
  }

  @GET
  @Path(value = "/data/filters")
  @Produces(MediaType.APPLICATION_JSON)
  public String getFilters(@QueryParam("dataset") String collection) {
    String jsonFilters = null;
    try {
      jsonFilters = dimensionFiltersCache.get(collection);
    } catch (ExecutionException e) {
      LOG.error("Exception while getting filters for collection {}", collection, e);
    }
    return jsonFilters;
  }


  @GET
  @Path(value = "/data/heatmap")
  @Produces(MediaType.APPLICATION_JSON)
  public String getHeatMap(@QueryParam("dataset") String collection,
      @QueryParam("filters") String filterJson,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("baselineStart") Long baselineStart, @QueryParam("baselineEnd") Long baselineEnd,
      @QueryParam("currentStart") Long currentStart, @QueryParam("currentEnd") Long currentEnd,
      @QueryParam("compareMode") String compareMode, @QueryParam("metrics") String metricsJson)
      throws Exception {

    HeatMapViewRequest request = new HeatMapViewRequest();

    request.setCollection(collection);
    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricsJson, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long maxDataTime = datasetMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      long delta = currentEnd - maxDataTime;
      currentEnd = currentEnd - delta;
      baselineEnd = baselineEnd - delta;
    }

    // See {@link #getDashboardData} for the reason that the start and end time are stored in a
    // DateTime object with data's timezone.
    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
    request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
    request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
    request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));
    // filter
    if (filterJson != null && !filterJson.isEmpty()) {
      filterJson = URLDecoder.decode(filterJson, "UTF-8");
      request.setFilters(ThirdEyeUtils.convertToMultiMap(filterJson));
    }

    HeatMapViewHandler handler = new HeatMapViewHandler(queryCache);
    HeatMapViewResponse response;
    String jsonResponse = null;

    try {
      response = handler.process(request);
      jsonResponse = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      LOG.debug("Heatmap response {}", jsonResponse);
    } catch (Exception e) {
      LOG.error("Error generating heatmap response", e);
    }

    return jsonResponse;
  }

  @GET
  @Path(value = "/data/tabular")
  @Produces(MediaType.APPLICATION_JSON)
  public String getTabularData(@QueryParam("dataset") String collection,
      @QueryParam("filters") String filterJson,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("baselineStart") Long baselineStart, @QueryParam("baselineEnd") Long baselineEnd,
      @QueryParam("currentStart") Long currentStart, @QueryParam("currentEnd") Long currentEnd,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("metrics") String metricsJson) throws Exception {

    TabularViewRequest request = new TabularViewRequest();
    request.setCollection(collection);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricsJson, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long maxDataTime = datasetMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      long delta = currentEnd - maxDataTime;
      currentEnd = currentEnd - delta;
      baselineEnd = baselineEnd - delta;
    }

    // See {@link #getDashboardData} for the reason that the start and end time are stored in a
    // DateTime object with data's timezone.
    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
    request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
    request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
    request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

    if (filterJson != null && !filterJson.isEmpty()) {
      filterJson = URLDecoder.decode(filterJson, "UTF-8");
      request.setFilters(ThirdEyeUtils.convertToMultiMap(filterJson));
    }
    request.setTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));

    TabularViewHandler handler = new TabularViewHandler(queryCache);
    String jsonResponse = null;

    try {
      TabularViewResponse response = handler.process(request);
      jsonResponse = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response);
      LOG.debug("Tabular response {}", jsonResponse);
    } catch (Exception e) {
      LOG.error("Exception while processing /data/tabular call", e);
    }

    return jsonResponse;
  }

  @GET
  @Path(value = "/data/contributor")
  @Produces(MediaType.APPLICATION_JSON)
  public String getContributorData(@QueryParam("dataset") String collection,
      @QueryParam("filters") String filterJson,
      @QueryParam("timeZone") @DefaultValue(DEFAULT_TIMEZONE_ID) String timeZone,
      @QueryParam("baselineStart") Long baselineStart, @QueryParam("baselineEnd") Long baselineEnd,
      @QueryParam("currentStart") Long currentStart, @QueryParam("currentEnd") Long currentEnd,
      @QueryParam("compareMode") String compareMode,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("metrics") String metricsJson, @QueryParam("dimensions") String groupByDimensions)
      throws Exception {

    ContributorViewRequest request = new ContributorViewRequest();
    request.setCollection(collection);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricsJson, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long maxDataTime = datasetMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      long delta = currentEnd - maxDataTime;
      currentEnd = currentEnd - delta;
      baselineEnd = baselineEnd - delta;
    }

    // See {@link #getDashboardData} for the reason that the start and end time are stored in a
    // DateTime object with data's timezone.
    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
    request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
    request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
    request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

    if (filterJson != null && !filterJson.isEmpty()) {
      filterJson = URLDecoder.decode(filterJson, "UTF-8");
      request.setFilters(ThirdEyeUtils.convertToMultiMap(filterJson));
    }
    request.setTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    if (groupByDimensions != null && !groupByDimensions.isEmpty()) {
      request.setGroupByDimensions(Arrays.asList(groupByDimensions.trim().split(",")));
    }
    ContributorViewHandler handler = new ContributorViewHandler(queryCache);
    String jsonResponse = null;

    try {
      ContributorViewResponse response = handler.process(request);
      jsonResponse =
          OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(response);
      LOG.debug("Contributor response {}", jsonResponse);

    } catch (Exception e) {
      LOG.error("Exception while processing /data/tabular call", e);
    }

    return jsonResponse;
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
  @Path(value = "/thirdeye")
  @Produces(MediaType.APPLICATION_JSON)
  public String saySomethingAwesome(@QueryParam("praise") String praise) {
    JSONObject hello = new JSONObject();
    try {
      hello.put("thirdeye", praise);
    } catch (JSONException e) {
      // TODO Auto-generated catch block
    }
    return hello.toString();
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

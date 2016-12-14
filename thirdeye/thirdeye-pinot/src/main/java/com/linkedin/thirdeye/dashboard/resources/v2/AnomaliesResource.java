package com.linkedin.thirdeye.dashboard.resources.v2;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/anomalies")
@Produces(MediaType.APPLICATION_JSON)
public class AnomaliesResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomaliesResource.class);
  private static final String START_END_DATE_FORMAT_DAYS = "MMM d yyyy";
  private static final String START_END_DATE_FORMAT_HOURS = "MMM d yyyy HH:mm";
  private static final String TIME_SERIES_DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private static final String ANOMALY_BASELINE_VAL_KEY = "baseLineVal";
  private static final String ANOMALY_CURRENT_VAL_KEY = "currentVal";
  private static final String COMMA_SEPARATOR = ",";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DashboardConfigManager dashboardConfigDAO;

  public AnomaliesResource() {
    metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    dashboardConfigDAO = DAO_REGISTRY.getDashboardConfigDAO();
  }

  @GET
  @Path("autocomplete/anomalyId")
  public List<String> getAnomaliesWhereAnomalyIdLike(@QueryParam("id") String id) {
    return mergedAnomalyResultDAO.findAllIdsLike(id);
  }

  @GET
  @Path("metric/id/{metricId}/{startTime}/{endTime}")
  public List<MergedAnomalyResultDTO> getAnomaliesForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) {

    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
    String dataset = metricConfig.getDataset();
    String metric = metricConfig.getName();
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(metric, dataset, startTime, endTime);

    return mergedAnomalies;
  }

  @GET
  @Path("metric/name/{dataset}/{metricName}/{startTime}/{endTime}")
  public List<MergedAnomalyResultDTO> getAnomaliesForMetricInRange(
      @PathParam("dataset") String dataset,
      @PathParam("metricName") String metricName,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) {

    List<MergedAnomalyResultDTO> mergedAnomalies =
        mergedAnomalyResultDAO.findByCollectionMetricTime(dataset, metricName, startTime, endTime, false);
    return mergedAnomalies;
  }


  @GET
  @Path("metric/count/{metricId}/{startTime}/{endTime}/{numBuckets}")
  public List<Integer> getAnomalyCountForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("numBuckets") int numBuckets) {

    Integer[] anomalyCount = new Integer[numBuckets];
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(metricId, startTime, endTime);

    long bucketSize = (endTime - startTime) / numBuckets;
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      Long anomalyStartTime = mergedAnomaly.getStartTime();
      Integer bucketNumber = (int) ((anomalyStartTime - startTime) / bucketSize);
      anomalyCount[bucketNumber] ++;
    }
    return Lists.newArrayList(anomalyCount);
  }

  @GET
  @Path("function/{mergedAnomalyId}")
  public AnomalyFunctionDTO getFunctionDetailsForMergedAnomaly(
      @PathParam("mergedAnomalyId") Long mergedAnomalyId) {
    MergedAnomalyResultDTO mergedAnomaly = mergedAnomalyResultDAO.findById(mergedAnomalyId);
    Long anomalyFunctionId = mergedAnomaly.getFunctionId();
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
    return anomalyFunction;
  }

  @GET
  @Path("search/anomalyIds/{startTime}/{endTime}")
  public List<AnomalyWrapper> getAnomaliesByAnomalyIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("anomalyIds") String anomalyIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    String[] anomalyIds = anomalyIdsString.split(COMMA_SEPARATOR);

    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();
    for (String id : anomalyIds) {
      Long anomalyId = Long.valueOf(id);
      mergedAnomalies.add(mergedAnomalyResultDAO.findById(anomalyId));
    }

    List<AnomalyWrapper> anomalyWrappers = new ArrayList<>();
    Map<Long, AnomalyFunctionDTO> anomalyFunctionStore = new HashMap<>();
    Map<Long, JSONObject> currentTimeSeriesStore = new HashMap<>();
    Map<Long, JSONObject> baselineTimeSeriesStore = new HashMap<>();

    // for each anomaly, fetch function details and create wrapper
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {

      String dataset = mergedAnomaly.getCollection();
      String metricName = mergedAnomaly.getMetric();
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
      DateTimeFormatter  timeSeriesDateFormatter = DateTimeFormat.forPattern(TIME_SERIES_DATE_FORMAT).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterDays = DateTimeFormat.forPattern(START_END_DATE_FORMAT_DAYS).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterHours = DateTimeFormat.forPattern(START_END_DATE_FORMAT_HOURS).withZone(Utils.getDataTimeZone(dataset));

      Long anomalyFunctionId = mergedAnomaly.getFunctionId();
      LOG.info("AnomalyFunctionId {} MergedAnomalyId {}", anomalyFunctionId, mergedAnomaly.getId());
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionStore.get(anomalyFunctionId);
      if (anomalyFunction == null) {
        anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
        anomalyFunctionStore.put(anomalyFunctionId, anomalyFunction);
      }
      String aggGranularity = constructAggGranularity(anomalyFunction);
      JSONObject currentTimeseriesResponse = currentTimeSeriesStore.get(anomalyFunctionId);
      if (currentTimeseriesResponse == null) {
        currentTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(), startTime, endTime,
            aggGranularity, metricName);
        currentTimeSeriesStore.put(anomalyFunctionId, currentTimeseriesResponse);
      }
      JSONObject baselineTimeseriesResponse = baselineTimeSeriesStore.get(anomalyFunctionId);
      long baselineStartTime = new DateTime(startTime).minusDays(7).getMillis();
      long baselineEndTime = startTime;
      if (baselineTimeseriesResponse == null) {
        baselineTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(),
            baselineStartTime, baselineEndTime, aggGranularity, metricName);
        baselineTimeSeriesStore.put(anomalyFunctionId, baselineTimeseriesResponse);
      }

      AnomalyWrapper anomalyWrapper = constructAnomalyWrapper(metricName, dataset, datasetConfig, mergedAnomaly,
          anomalyFunction, startTime, endTime, baselineStartTime, baselineEndTime,
          currentTimeseriesResponse, baselineTimeseriesResponse,
          timeSeriesDateFormatter, startEndDateFormatterHours, startEndDateFormatterDays);
      anomalyWrappers.add(anomalyWrapper);
    }


    return anomalyWrappers;
  }


  @GET
  @Path("search/dashboardId/{startTime}/{endTime}")
  public List<AnomalyWrapper> getAnomaliesByDashboardId(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("dashboardId") String dashboardId,
      @QueryParam("functionName") String functionName) throws Exception {

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.findById(Long.valueOf(dashboardId));
    String metricIdsString = Joiner.on(COMMA_SEPARATOR).join(dashboardConfig.getMetricIds());
    return getAnomaliesByMetricIds(startTime, endTime, metricIdsString, functionName);
  }

  @GET
  @Path("search/metricIds/{startTime}/{endTime}")
  public List<AnomalyWrapper> getAnomaliesByMetricIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    List<AnomalyWrapper> anomalyWrappers = new ArrayList<>();

    String[] metricIds = metricIdsString.split(COMMA_SEPARATOR);
    for (String id : metricIds) {

      Long metricId = Long.valueOf(id);
      MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
      if (metricConfig == null) {
        continue;
      }
      String metricName = metricConfig.getName();
      String dataset = metricConfig.getDataset();
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);

      DateTimeFormatter  timeSeriesDateFormatter = DateTimeFormat.forPattern(TIME_SERIES_DATE_FORMAT).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterDays = DateTimeFormat.forPattern(START_END_DATE_FORMAT_DAYS).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterHours = DateTimeFormat.forPattern(START_END_DATE_FORMAT_HOURS).withZone(Utils.getDataTimeZone(dataset));

      // fetch anomalies in range
      List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(dataset, metricName, startTime, endTime);

      Map<Long, AnomalyFunctionDTO> anomalyFunctionStore = new HashMap<>();
      Map<Long, JSONObject> currentTimeSeriesStore = new HashMap<>();
      Map<Long, JSONObject> baselineTimeSeriesStore = new HashMap<>();
      // for each anomaly, fetch function details and create wrapper
      for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {

        Long anomalyFunctionId = mergedAnomaly.getFunctionId();
        LOG.info("AnomalyFunctionId {} MergedAnomalyId {}", anomalyFunctionId, mergedAnomaly.getId());
        AnomalyFunctionDTO anomalyFunction = anomalyFunctionStore.get(anomalyFunctionId);
        if (anomalyFunction == null) {
          anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
          anomalyFunctionStore.put(anomalyFunctionId, anomalyFunction);
        }
        String aggGranularity = constructAggGranularity(anomalyFunction);
        JSONObject currentTimeseriesResponse = currentTimeSeriesStore.get(anomalyFunctionId);
        if (currentTimeseriesResponse == null) {
          currentTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(), startTime, endTime,
              aggGranularity, metricName);
          currentTimeSeriesStore.put(anomalyFunctionId, currentTimeseriesResponse);
        }
        JSONObject baselineTimeseriesResponse = baselineTimeSeriesStore.get(anomalyFunctionId);
        long baselineStartTime = new DateTime(startTime).minusDays(7).getMillis();
        long baselineEndTime = startTime;
        if (baselineTimeseriesResponse == null) {
          baselineTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(),
              baselineStartTime, baselineEndTime, aggGranularity, metricName);
          baselineTimeSeriesStore.put(anomalyFunctionId, baselineTimeseriesResponse);
        }

        AnomalyWrapper anomalyWrapper = constructAnomalyWrapper(metricName, dataset, datasetConfig,
            mergedAnomaly, anomalyFunction,
            startTime, endTime, baselineStartTime, baselineEndTime,
            currentTimeseriesResponse, baselineTimeseriesResponse,
            timeSeriesDateFormatter, startEndDateFormatterHours, startEndDateFormatterDays);
        anomalyWrappers.add(anomalyWrapper);
      }
    }
    return anomalyWrappers;
  }


  public JSONObject getTimeSeriesData(String collection, Multimap<String, String> filters,
      Long start, Long end, String aggTimeGranularity, String metric) throws Exception {

    TimeSeriesRequest request = new TimeSeriesRequest();
    request.setCollectionName(collection);

    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setStart(new DateTime(start, timeZoneForCollection));
    request.setEnd(new DateTime(end, timeZoneForCollection));

    request.setFilterSet(filters);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);

    request.setAggregationTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(collection);
    TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);

    if (!request.getAggregationTimeGranularity().getUnit().equals(TimeUnit.DAYS) ||
        !StringUtils.isBlank(timespec.getFormat())) {
      request.setEndDateInclusive(true);
    }

    TimeSeriesHandler handler = new TimeSeriesHandler(CACHE_REGISTRY.getQueryCache());
    JSONObject jsonResponseObject = new JSONObject();
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

      jsonResponseObject.put("timeSeriesData", timeseriesMap);
      jsonResponseObject.put("keys", new JSONArray(keys));
      jsonResponseObject.put("summary", summaryMap);
    } catch (Exception e) {
      throw e;
    }
    LOG.info("Response:{}", jsonResponseObject);
    return jsonResponseObject;
  }



  @GET
  @Path(value = "/data/timeseries")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getTimeSeriesData(@QueryParam("dataset") String collection,
      @QueryParam("filters") String filterJson,
      @QueryParam("currentStart") Long start, @QueryParam("currentEnd") Long end,
      @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("metric") String metric)
      throws Exception {

    Multimap<String, String> filters = null;
    if (filterJson != null && !filterJson.isEmpty()) {
      filterJson = URLDecoder.decode(filterJson, "UTF-8");
      filters = ThirdEyeUtils.convertToMultiMap(filterJson);
    }
    JSONObject jsonResponseObject = getTimeSeriesData(collection, filters, start, end, aggTimeGranularity, metric);
    return jsonResponseObject;
  }

  private List<String> getDataFromTimeSeriesObject(JSONObject timeSeriesResponse, String metricName) throws JSONException {
    JSONObject timeSeriesMap = (JSONObject) timeSeriesResponse.get("timeSeriesData");
    JSONArray valueArray = (JSONArray) timeSeriesMap.get(metricName);
    List<String> list = new ArrayList<String>();
    for (int i = 0; i< valueArray.length(); i++) {
        list.add(valueArray.getString(i));
    }
    LOG.info("List {}", list);
    return list;
  }

  private List<String> getDateFromTimeSeriesObject(JSONObject timeSeriesResponse, DateTimeFormatter timeSeriesDateFormatter) throws JSONException {

    JSONObject timeSeriesMap = (JSONObject) timeSeriesResponse.get("timeSeriesData");
    JSONArray valueArray = (JSONArray) timeSeriesMap.get("time");
    List<String> list = new ArrayList<String>();
    for (int i = 0; i< valueArray.length(); i++) {
        list.add(timeSeriesDateFormatter.print(Long.valueOf(valueArray.getString(i))));
    }
    LOG.info("List {}", list);
    return list;
  }

  private Map<String, String> getMessageDataMap(String message) {
    Map<String, String> messageDataMap = new HashMap<>();
    String[] tokens = message.split("[,:]");
    for (int i = 0; i < tokens.length; i = i+2) {
      messageDataMap.put(tokens[i].trim(), tokens[i+1].trim());
    }
    LOG.info("Map {}", messageDataMap);
    return messageDataMap;
  }

  private String constructAggGranularity(AnomalyFunctionDTO anomalyFunction) {
    String aggGranularity = anomalyFunction.getBucketSize() + "_" + anomalyFunction.getBucketUnit();
    return aggGranularity;
  }

  private String getFormattedDateTime(long timestamp, DatasetConfigDTO datasetConfig,
      DateTimeFormatter startEndDateFormatterHours, DateTimeFormatter startEndDateFormatterDays) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataGranularityUnit = timeSpec.getDataGranularity().getUnit();
    String formattedDateTime = null;
    if (dataGranularityUnit.equals(TimeUnit.MINUTES) || dataGranularityUnit.equals(TimeUnit.HOURS)) {
      formattedDateTime = startEndDateFormatterHours.print(timestamp);
    } else if (dataGranularityUnit.equals(TimeUnit.DAYS)) {
      formattedDateTime = startEndDateFormatterDays.print(timestamp);
    }
    return formattedDateTime;
  }

  private AnomalyWrapper constructAnomalyWrapper(String metricName, String dataset, DatasetConfigDTO datasetConfig,
      MergedAnomalyResultDTO mergedAnomaly, AnomalyFunctionDTO anomalyFunction,
      long currentStartTime, long currentEndTime, long baselineStartTime, long baselineEndTime,
      JSONObject currentTimeseriesResponse, JSONObject baselineTimeseriesResponse,
      DateTimeFormatter timeSeriesDateFormatter, DateTimeFormatter startEndDateFormatterHours, DateTimeFormatter startEndDateFormatterDays) throws JSONException {

    AnomalyWrapper anomalyWrapper = new AnomalyWrapper();
    anomalyWrapper.setMetric(metricName);
    anomalyWrapper.setDataset(dataset);

    // get this from timeseries calls
    List<String> dateValues = getDateFromTimeSeriesObject(currentTimeseriesResponse, timeSeriesDateFormatter);
    anomalyWrapper.setDates(dateValues);
    anomalyWrapper.setCurrentEnd(getFormattedDateTime(currentEndTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyWrapper.setCurrentStart(getFormattedDateTime(currentStartTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyWrapper.setBaselineEnd(getFormattedDateTime(baselineEndTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyWrapper.setBaselineStart(getFormattedDateTime(baselineStartTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    List<String> baselineValues = getDataFromTimeSeriesObject(baselineTimeseriesResponse, metricName);
    anomalyWrapper.setBaselineValues(baselineValues);
    List<String> currentValues = getDataFromTimeSeriesObject(currentTimeseriesResponse, metricName);
    anomalyWrapper.setCurrentValues(currentValues);

    // from function and anomaly
    anomalyWrapper.setAnomalyId(mergedAnomaly.getId());
    anomalyWrapper.setAnomalyRegionStart(timeSeriesDateFormatter.print(Long.valueOf(mergedAnomaly.getStartTime())));
    anomalyWrapper.setAnomalyRegionEnd(timeSeriesDateFormatter.print(Long.valueOf(mergedAnomaly.getEndTime())));
    Map<String, String> messageDataMap = getMessageDataMap(mergedAnomaly.getMessage());
    anomalyWrapper.setCurrent(messageDataMap.get(ANOMALY_CURRENT_VAL_KEY));
    anomalyWrapper.setBaseline(messageDataMap.get(ANOMALY_BASELINE_VAL_KEY));
    anomalyWrapper.setAnomalyFunctionId(anomalyFunction.getId());
    anomalyWrapper.setAnomalyFunctionName(anomalyFunction.getFunctionName());
    anomalyWrapper.setAnomalyFunctionType(anomalyFunction.getType());
    anomalyWrapper.setAnomalyFunctionProps(anomalyFunction.getProperties());
    anomalyWrapper.setAnomalyFunctionDimension(mergedAnomaly.getDimensions().toString());
    if (mergedAnomaly.getFeedback() != null) {
      anomalyWrapper.setAnomalyFeedback(mergedAnomaly.getFeedback().getStatus().toString());
    }
    return anomalyWrapper;
  }


}

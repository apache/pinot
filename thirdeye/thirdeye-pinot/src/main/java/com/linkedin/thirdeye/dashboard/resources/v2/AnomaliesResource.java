package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomaliesSummary;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomaliesWrapper;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomalyDetails;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
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
  private static final int DEFAULT_PAGE_NUMBER = 1;
  private static final int DEFAULT_PAGE_SIZE = 10;

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


  /** Find anomalies for metric id in time range
   *
   * @param metricId
   * @param startTime
   * @param endTime
   * @return
   */
  @GET
  @Path("metric/id/{metricId}/{startTime}/{endTime}")
  public List<MergedAnomalyResultDTO> getAnomaliesForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) {

    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
    String dataset = metricConfig.getDataset();
    String metric = metricConfig.getName();
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(dataset, metric, startTime, endTime);

    return mergedAnomalies;
  }

  /**
   * Find anomalies for metric in time range
   * @param dataset
   * @param metricName
   * @param startTime
   * @param endTime
   * @return
   */
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



  /**
   * Get count of anomalies for metric in time range, divided into specified number of buckets
   * @param metricId
   * @param startTime
   * @param endTime
   * @param numBuckets
   * @return
   */
  @GET
  @Path("metric/count/{metricId}/{startTime}/{endTime}")
  public AnomaliesSummary getAnomalyCountForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime){

    AnomaliesSummary anomaliesSummary = new AnomaliesSummary();
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(metricId, startTime, endTime);

    int resolvedAnomalies = 0;
    int unresolvedAnomalies = 0;
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      AnomalyFeedbackDTO anomalyFeedback = mergedAnomaly.getFeedback();
      if (anomalyFeedback == null || anomalyFeedback.getFeedbackType() == null) {
        unresolvedAnomalies ++;
      } else if (anomalyFeedback != null && anomalyFeedback.getFeedbackType() != null
          && anomalyFeedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
        unresolvedAnomalies ++;
      } else {
        resolvedAnomalies ++;
      }
    }
    anomaliesSummary.setMetricId(metricId);
    anomaliesSummary.setStartTime(startTime);
    anomaliesSummary.setEndTime(endTime);
    anomaliesSummary.setNumAnomalies(mergedAnomalies.size());
    anomaliesSummary.setNumAnomaliesResolved(resolvedAnomalies);
    anomaliesSummary.setNumAnomaliesUnresolved(unresolvedAnomalies);
    return anomaliesSummary;
  }

  /**
   * Get anomaly function details for merged anomaly
   * @param mergedAnomalyId
   * @return
   */
  @GET
  @Path("function/{mergedAnomalyId}")
  public AnomalyFunctionDTO getFunctionDetailsForMergedAnomaly(
      @PathParam("mergedAnomalyId") Long mergedAnomalyId) {
    MergedAnomalyResultDTO mergedAnomaly = mergedAnomalyResultDAO.findById(mergedAnomalyId);
    Long anomalyFunctionId = mergedAnomaly.getFunctionId();
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
    return anomalyFunction;
  }

  /**
   * Find anomalies by anomaly ids
   * @param startTime
   * @param endTime
   * @param anomalyIdsString
   * @param functionName
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/anomalyIds/{startTime}/{endTime}")
  public AnomaliesWrapper getAnomaliesByAnomalyIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("anomalyIds") String anomalyIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    AnomaliesWrapper anomaliesWrapper = new AnomaliesWrapper();
    String[] anomalyIds = anomalyIdsString.split(COMMA_SEPARATOR);

    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();
    for (String id : anomalyIds) {
      Long anomalyId = Long.valueOf(id);
      mergedAnomalies.add(mergedAnomalyResultDAO.findById(anomalyId));
    }

    List<AnomalyDetails> anomalyDetailsList = new ArrayList<>();
    // for each anomaly, fetch function details and create wrapper
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {

      String dataset = mergedAnomaly.getCollection();
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
      DateTimeFormatter  timeSeriesDateFormatter = DateTimeFormat.forPattern(TIME_SERIES_DATE_FORMAT).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterDays = DateTimeFormat.forPattern(START_END_DATE_FORMAT_DAYS).withZone(Utils.getDataTimeZone(dataset));
      DateTimeFormatter startEndDateFormatterHours = DateTimeFormat.forPattern(START_END_DATE_FORMAT_HOURS).withZone(Utils.getDataTimeZone(dataset));

      AnomalyDetails anomalyDetails = getAnomalyDetails(mergedAnomaly, datasetConfig, timeSeriesDateFormatter,
          startEndDateFormatterHours, startEndDateFormatterDays);
      anomalyDetailsList.add(anomalyDetails);
    }
    anomaliesWrapper.setTotalAnomalies(anomalyDetailsList.size());
    anomaliesWrapper.setNumAnomaliesOnPage(anomalyDetailsList.size());
    anomaliesWrapper.setAnomalyDetailsList(anomalyDetailsList);
    return anomaliesWrapper;
  }


  /**
   * Find anomalies by dashboard id
   * @param startTime
   * @param endTime
   * @param dashboardId
   * @param functionName
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/dashboardId/{startTime}/{endTime}")
  public AnomaliesWrapper getAnomaliesByDashboardId(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("dashboardId") String dashboardId,
      @QueryParam("functionName") String functionName) throws Exception {

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.findById(Long.valueOf(dashboardId));
    String metricIdsString = Joiner.on(COMMA_SEPARATOR).join(dashboardConfig.getMetricIds());
    return getAnomaliesByMetricIds(startTime, endTime, metricIdsString, functionName);
  }

  /**
   * Find anomalies by metric ids
   * @param startTime
   * @param endTime
   * @param metricIdsString
   * @param functionName
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/metricIds/{startTime}/{endTime}")
  public AnomaliesWrapper getAnomaliesByMetricIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    AnomaliesWrapper anomaliesWrapper = new AnomaliesWrapper();
    List<AnomalyDetails> anomalyDetailsList = new ArrayList<>();

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

      // fetch anomalies in range, ordered by end time
      List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(dataset, metricName, startTime, endTime);

      // for each anomaly, fetch function details and create wrapper
      for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {

        AnomalyDetails anomalyDetails = getAnomalyDetails(mergedAnomaly, datasetConfig, timeSeriesDateFormatter,
            startEndDateFormatterHours, startEndDateFormatterDays);
        anomalyDetailsList.add(anomalyDetails);
      }
    }
    anomaliesWrapper.setTotalAnomalies(anomalyDetailsList.size());
    // TODO: get pagenumber and pagesize from client
    int pageNumber = DEFAULT_PAGE_NUMBER;
    int pageSize = DEFAULT_PAGE_SIZE;
    int maxPageNumber = anomalyDetailsList.size()/pageSize + 1;
    if (pageNumber > maxPageNumber) {
      pageNumber = maxPageNumber;
    }
    if (pageNumber < 1) {
      pageNumber = 1;
    }
    if (anomalyDetailsList.size() > pageSize) {
      if (anomalyDetailsList.size() > pageSize * pageNumber ) {
        anomalyDetailsList.subList(pageSize * pageNumber, anomalyDetailsList.size()).clear();
      }
      anomalyDetailsList.subList(0, pageSize * (pageNumber - 1)).clear();
    }
    anomaliesWrapper.setNumAnomaliesOnPage(anomalyDetailsList.size());
    anomaliesWrapper.setAnomalyDetailsList(anomalyDetailsList);
    return anomaliesWrapper;
  }


  /**
   * Get timeseries for metric
   * @param collection
   * @param filterJson
   * @param start
   * @param end
   * @param aggTimeGranularity
   * @param metric
   * @return
   * @throws Exception
   */
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

  /**
   * Update anomaly feedback
   * @param mergedAnomalyId : mergedAnomalyId
   * @param payload         : Json payload containing feedback @see com.linkedin.thirdeye.constant.AnomalyFeedbackType
   *                        eg. payload
   *                        <p/>
   *                        { "feedbackType": "NOT_ANOMALY", "comment": "this is not an anomaly" }
   */
  @POST
  @Path(value = "/updateFeedback/{mergedAnomalyId}")
  public void updateAnomalyMergedResultFeedback(@PathParam("mergedAnomalyId") long mergedAnomalyId, String payload) {
    try {
      MergedAnomalyResultDTO result = mergedAnomalyResultDAO.findById(mergedAnomalyId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + mergedAnomalyId);
      }
      AnomalyFeedbackDTO feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      AnomalyFeedbackDTO feedbackRequest = new ObjectMapper().readValue(payload, AnomalyFeedbackDTO.class);
      if (feedbackRequest.getStatus() == null) {
        feedback.setStatus(FeedbackStatus.NEW);
      } else {
        feedback.setStatus(feedbackRequest.getStatus());
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());
      mergedAnomalyResultDAO.updateAnomalyFeedback(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }

  // ----------- HELPER FUNCTIONS
  /**
   * Get timeseries for metric
   * @param collection
   * @param filters
   * @param start
   * @param end
   * @param aggTimeGranularity
   * @param metric
   * @return
   * @throws Exception
   */
  private JSONObject getTimeSeriesData(String collection, Multimap<String, String> filters,
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

  /**
   * Extract data values form timeseries object
   * @param timeSeriesResponse
   * @param metricName
   * @return
   * @throws JSONException
   */
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

  /**
   * Extract date values from time series object
   * @param timeSeriesResponse
   * @param timeSeriesDateFormatter
   * @return
   * @throws JSONException
   */
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


  private Map<String, String> getAnomalyMessageDataMap(String message) {
    Map<String, String> messageDataMap = new HashMap<>();
    String[] tokens = message.split("[,:]");
    for (int i = 0; i < tokens.length; i = i+2) {
      messageDataMap.put(tokens[i].trim(), tokens[i+1].trim());
    }
    LOG.info("Map {}", messageDataMap);
    return messageDataMap;
  }

  /**
   * Construct agg granularity for using in timeseries
   * @param anomalyFunction
   * @return
   */
  private String constructAggGranularity(DatasetConfigDTO datasetConfig) {
    String aggGranularity = datasetConfig.getTimeDuration() + "_" + datasetConfig.getTimeUnit();
    return aggGranularity;
  }

  /**
   * Get formatted date time for anomaly chart
   * @param timestamp
   * @param datasetConfig
   * @param startEndDateFormatterHours
   * @param startEndDateFormatterDays
   * @return
   */
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


  private AnomalyDetails getAnomalyDetails(MergedAnomalyResultDTO mergedAnomaly, DatasetConfigDTO datasetConfig,
      DateTimeFormatter timeSeriesDateFormatter, DateTimeFormatter startEndDateFormatterHours,
      DateTimeFormatter startEndDateFormatterDays) {

    String dataset = datasetConfig.getDataset();
    String metricName = mergedAnomaly.getMetric();

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(mergedAnomaly.getFunctionId());

    String aggGranularity = constructAggGranularity(datasetConfig);

    long anomalyStartTime = mergedAnomaly.getStartTime();
    long anomalyEndTime = mergedAnomaly.getEndTime();
    TimeRange range = getTimeseriesOffsetedTimes(anomalyStartTime, anomalyEndTime, datasetConfig);
    long currentStartTime = range.getStart();
    long currentEndTime = range.getEnd();
    long baselineStartTime = new DateTime(currentStartTime).minusDays(7).getMillis();
    long baselineEndTime = new DateTime(currentEndTime).minusDays(7).getMillis();

    AnomalyDetails anomalyDetails = null;
    try {
    JSONObject currentTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(),
        currentStartTime, currentEndTime, aggGranularity, metricName);
    JSONObject baselineTimeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(),
          baselineStartTime, baselineEndTime, aggGranularity, metricName);

    anomalyDetails = constructAnomalyDetails(metricName, dataset, datasetConfig,
        mergedAnomaly, anomalyFunction,
        currentStartTime, currentEndTime, baselineStartTime, baselineEndTime,
        currentTimeseriesResponse, baselineTimeseriesResponse,
        timeSeriesDateFormatter, startEndDateFormatterHours, startEndDateFormatterDays);
    } catch (Exception e) {
      LOG.error("Exception in constructing anomaly wrapper for anomaly {}", mergedAnomaly.getId(), e);
    }
    return anomalyDetails;
  }

  /** Construct anomaly wrapper using all details fetched from calls
   *
   * @param metricName
   * @param dataset
   * @param datasetConfig
   * @param mergedAnomaly
   * @param anomalyFunction
   * @param currentStartTime
   * @param currentEndTime
   * @param baselineStartTime
   * @param baselineEndTime
   * @param currentTimeseriesResponse
   * @param baselineTimeseriesResponse
   * @param timeSeriesDateFormatter
   * @param startEndDateFormatterHours
   * @param startEndDateFormatterDays
   * @return
   * @throws JSONException
   */
  private AnomalyDetails constructAnomalyDetails(String metricName, String dataset, DatasetConfigDTO datasetConfig,
      MergedAnomalyResultDTO mergedAnomaly, AnomalyFunctionDTO anomalyFunction,
      long currentStartTime, long currentEndTime, long baselineStartTime, long baselineEndTime,
      JSONObject currentTimeseriesResponse, JSONObject baselineTimeseriesResponse,
      DateTimeFormatter timeSeriesDateFormatter, DateTimeFormatter startEndDateFormatterHours, DateTimeFormatter startEndDateFormatterDays) throws JSONException {

    AnomalyDetails anomalyDetails = new AnomalyDetails();
    anomalyDetails.setMetric(metricName);
    anomalyDetails.setDataset(dataset);

    // get this from timeseries calls
    List<String> dateValues = getDateFromTimeSeriesObject(currentTimeseriesResponse, timeSeriesDateFormatter);
    anomalyDetails.setDates(dateValues);
    anomalyDetails.setCurrentEnd(getFormattedDateTime(currentEndTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyDetails.setCurrentStart(getFormattedDateTime(currentStartTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyDetails.setBaselineEnd(getFormattedDateTime(baselineEndTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    anomalyDetails.setBaselineStart(getFormattedDateTime(baselineStartTime, datasetConfig, startEndDateFormatterHours, startEndDateFormatterDays));
    List<String> baselineValues = getDataFromTimeSeriesObject(baselineTimeseriesResponse, metricName);
    anomalyDetails.setBaselineValues(baselineValues);
    List<String> currentValues = getDataFromTimeSeriesObject(currentTimeseriesResponse, metricName);
    anomalyDetails.setCurrentValues(currentValues);

    // from function and anomaly
    anomalyDetails.setAnomalyId(mergedAnomaly.getId());
    anomalyDetails.setAnomalyRegionStart(timeSeriesDateFormatter.print(Long.valueOf(mergedAnomaly.getStartTime())));
    anomalyDetails.setAnomalyRegionEnd(timeSeriesDateFormatter.print(Long.valueOf(mergedAnomaly.getEndTime())));
    Map<String, String> messageDataMap = getAnomalyMessageDataMap(mergedAnomaly.getMessage());
    anomalyDetails.setCurrent(messageDataMap.get(ANOMALY_CURRENT_VAL_KEY));
    anomalyDetails.setBaseline(messageDataMap.get(ANOMALY_BASELINE_VAL_KEY));
    anomalyDetails.setAnomalyFunctionId(anomalyFunction.getId());
    anomalyDetails.setAnomalyFunctionName(anomalyFunction.getFunctionName());
    anomalyDetails.setAnomalyFunctionType(anomalyFunction.getType());
    anomalyDetails.setAnomalyFunctionProps(anomalyFunction.getProperties());
    anomalyDetails.setAnomalyFunctionDimension(mergedAnomaly.getDimensions().toString());
    if (mergedAnomaly.getFeedback() != null) {
      anomalyDetails.setAnomalyFeedback(AnomalyDetails.getFeedbackStringFromFeedbackType(mergedAnomaly.getFeedback().getFeedbackType()));
    }
    return anomalyDetails;
  }


  private TimeRange getTimeseriesOffsetedTimes(long anomalyStartTime, long anomalyEndTime, DatasetConfigDTO datasetConfig) {
    TimeUnit dataTimeunit = datasetConfig.getTimeUnit();
    long offsetMillis = 0;
    switch (dataTimeunit) {
      case DAYS: // 2 days
        offsetMillis = TimeUnit.MILLISECONDS.convert(2, dataTimeunit);
        break;
      case HOURS: // 10 hours
        offsetMillis = TimeUnit.MILLISECONDS.convert(10, dataTimeunit);
        break;
      case MINUTES: // 60 minutes
        offsetMillis = TimeUnit.MILLISECONDS.convert(60, dataTimeunit);
        break;
      default:
        break;
    }
    anomalyStartTime = anomalyStartTime - offsetMillis;
    anomalyEndTime = anomalyEndTime + offsetMillis;
    try {
      Long maxDataTime = CACHE_REGISTRY.getCollectionMaxDataTimeCache().get(datasetConfig.getDataset());
      if (anomalyEndTime > maxDataTime) {
        anomalyEndTime = maxDataTime;
      }
    } catch (ExecutionException e) {
      LOG.error("Exception when reading max time for {}", datasetConfig.getDataset(), e);
    }
    TimeRange range = new TimeRange(anomalyStartTime, anomalyEndTime);
    return range;
  }

}

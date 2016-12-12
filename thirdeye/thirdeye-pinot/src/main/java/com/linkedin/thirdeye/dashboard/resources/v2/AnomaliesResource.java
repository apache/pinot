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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Lists;

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
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/anomalies")
@Produces(MediaType.APPLICATION_JSON)
public class AnomaliesResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomaliesResource.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;

  public AnomaliesResource() {
    metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
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
  @Path("wrapper/{dataset}/{metricName}/{startTime}/{endTime}")
  public List<AnomalyWrapper> getAnomalyWrapper(
      @PathParam("dataset") String dataset,
      @PathParam("metricName") String metricName,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) throws Exception {

    List<AnomalyWrapper> anomalyWrappers = new ArrayList<>();

    // fetch anomalies in range
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(dataset, metricName, startTime, endTime);

    Map<Long, AnomalyFunctionDTO> anomalyFunctionStore = new HashMap<>();
    Map<Long, String> timeSeriesStore = new HashMap<>();
    // for each anomaly, fetch function details and create wrapper
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {

      Long anomalyFunctionId = mergedAnomaly.getFunctionId();
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionStore.get(anomalyFunctionId);
      if (anomalyFunction == null) {
        anomalyFunction = anomalyFunctionDAO.findById(anomalyFunctionId);
      }
      String timeseriesResponse = timeSeriesStore.get(anomalyFunctionId);
      if (timeseriesResponse == null) {
        timeseriesResponse = getTimeSeriesData(dataset, anomalyFunction.getFilterSet(), startTime, endTime,
            anomalyFunction.getBucketSize() + "_" + anomalyFunction.getBucketUnit().toString(), metricName);
      }

      AnomalyWrapper anomalyWrapper = new AnomalyWrapper();
      anomalyWrapper.setMetric(metricName);
      anomalyWrapper.setDataset(dataset);

      // get this from timeseries calls
      anomalyWrapper.setDates(Lists.newArrayList("2016-01-01", "2016-01-02", "2016-01-03", "2016-01-04", "2016-01-05", "2016-01-06", "2016-01-07"));
      anomalyWrapper.setCurrentEnd("Jan 7 2016");
      anomalyWrapper.setCurrentStart("Jan 1 2016");
      anomalyWrapper.setBaselineEnd("Dec 31 2015");
      anomalyWrapper.setBaselineStart("Dec 25 2015");
      anomalyWrapper.setBaselineValues(Lists.newArrayList(35, 225, 200, 600, 170, 220, 70));
      anomalyWrapper.setCurrentValues(Lists.newArrayList(30, 200, 100, 400, 150, 250, 60));
      anomalyWrapper.setCurrent("1000");
      anomalyWrapper.setBaseline("2000");

      // from function and anomaly
      anomalyWrapper.setAnomalyId(mergedAnomaly.getId());
      anomalyWrapper.setAnomalyRegionStart(String.valueOf(mergedAnomaly.getStartTime()));
      anomalyWrapper.setAnomalyRegionEnd(String.valueOf(mergedAnomaly.getEndTime()));
      anomalyWrapper.setAnomalyFunctionId(anomalyFunctionId);
      anomalyWrapper.setAnomalyFunctionName(anomalyFunction.getFunctionName());
      anomalyWrapper.setAnomalyFunctionType(anomalyFunction.getType());
      anomalyWrapper.setAnomalyFunctionProps(anomalyFunction.getProperties());
      anomalyWrapper.setAnomalyFunctionDimension(mergedAnomaly.getDimensions().toString());
      if (mergedAnomaly.getFeedback() != null) {
        anomalyWrapper.setAnomalyFeedback(mergedAnomaly.getFeedback().getStatus().toString());
      }


      anomalyWrappers.add(anomalyWrapper);
    }



    return anomalyWrappers;

  }


  public String getTimeSeriesData(String collection, Multimap<String, String> filters,
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
  @Path(value = "/data/timeseries")
  @Produces(MediaType.APPLICATION_JSON)
  public String getTimeSeriesData(@QueryParam("dataset") String collection,
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
    String jsonResponse = getTimeSeriesData(collection, filters, start, end, aggTimeGranularity, metric);
    LOG.info("Response:{}", jsonResponse);
    return jsonResponse;
  }


}

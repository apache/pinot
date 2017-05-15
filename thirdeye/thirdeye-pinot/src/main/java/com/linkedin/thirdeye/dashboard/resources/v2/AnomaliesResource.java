package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomaly.merge.TimeBasedAnomalyMerger;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomaliesSummary;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomaliesWrapper;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomalyDataCompare;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomalyDetails;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
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
import com.linkedin.thirdeye.util.AnomalyOffset;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/anomalies")
@Produces(MediaType.APPLICATION_JSON)
public class AnomaliesResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomaliesResource.class);
  private static final String START_END_DATE_FORMAT_DAYS = "MMM d yyyy";
  private static final String START_END_DATE_FORMAT_HOURS = "MMM d yyyy HH:mm";
  private static final String TIME_SERIES_DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private static final String COMMA_SEPARATOR = ",";
  private static final int DEFAULT_PAGE_SIZE = 10;
  private static final int NUM_EXECS = 40;
  private static final DateTimeZone DEFAULT_DASHBOARD_TIMEZONE = DateTimeZone.forID("America/Los_Angeles");
  private static final DateTimeFormatter  timeSeriesDateFormatter = DateTimeFormat.forPattern(TIME_SERIES_DATE_FORMAT).withZone(DEFAULT_DASHBOARD_TIMEZONE);
  private static final DateTimeFormatter startEndDateFormatterDays = DateTimeFormat.forPattern(START_END_DATE_FORMAT_DAYS).withZone(DEFAULT_DASHBOARD_TIMEZONE);
  private static final DateTimeFormatter startEndDateFormatterHours = DateTimeFormat.forPattern(START_END_DATE_FORMAT_HOURS).withZone(DEFAULT_DASHBOARD_TIMEZONE);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final OverrideConfigManager overrideConfigDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DashboardConfigManager dashboardConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private ExecutorService threadPool;
  private AlertFilterFactory alertFilterFactory;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  public AnomaliesResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    dashboardConfigDAO = DAO_REGISTRY.getDashboardConfigDAO();
    datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    threadPool = Executors.newFixedThreadPool(NUM_EXECS);
    this.alertFilterFactory = alertFilterFactory;
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  @GET
  @Path("/{anomalyId}")
  public AnomalyDataCompare.Response getAnomalyDataCompareResults(
      @PathParam("anomalyId") Long anomalyId) {
    MergedAnomalyResultDTO anomaly = mergedAnomalyResultDAO.findById(anomalyId);
    if (anomaly == null) {
      LOG.error("Anomaly not found with id " + anomalyId);
      throw new IllegalArgumentException("Anomaly not found with id " + anomalyId);
    }
    AnomalyDataCompare.Response response = new AnomalyDataCompare.Response();
    response.setCurrentStart(anomaly.getStartTime());
    response.setCurrenEnd(anomaly.getEndTime());
    try {
      DatasetConfigDTO dataset = datasetConfigDAO.findByDataset(anomaly.getCollection());
      DateTimeZone dateTimeZone = Utils.getDataTimeZone(anomaly.getCollection());
      TimeGranularity granularity = dataset.bucketTimeGranularity();

      // Lets compute currentTimeRange
      Pair<Long, Long> currentTimeRange = new Pair<>(anomaly.getStartTime(), anomaly.getEndTime());
      MetricTimeSeries currentTimeSeries = TimeSeriesUtil
          .getTimeSeriesByDimension(anomaly.getFunction(), Arrays.asList(currentTimeRange),
              anomaly.getDimensions(), granularity, false);
      double currentVal = getTotalFromTimeSeries(currentTimeSeries, dataset.isAdditive());
      response.setCurrentVal(currentVal);

      for (AlertConfigBean.COMPARE_MODE compareMode : AlertConfigBean.COMPARE_MODE.values()) {
        Period baselineOffsetPeriod = ThirdEyeUtils.getbaselineOffsetPeriodByMode(compareMode);
        DateTime anomalyStartTimeOffset = new DateTime(anomaly.getStartTime(), dateTimeZone).minus(baselineOffsetPeriod);
        DateTime anomalyEndTimeOffset = new DateTime(anomaly.getEndTime(), dateTimeZone).minus(baselineOffsetPeriod);
        Pair<Long, Long> baselineTimeRange =
            new Pair<>(anomalyStartTimeOffset.getMillis(), anomalyEndTimeOffset.getMillis());
        MetricTimeSeries baselineTimeSeries = TimeSeriesUtil
            .getTimeSeriesByDimension(anomaly.getFunction(), Arrays.asList(baselineTimeRange),
                anomaly.getDimensions(), granularity, false);
        AnomalyDataCompare.CompareResult compareResult = new AnomalyDataCompare.CompareResult();
        double baseLineval = getTotalFromTimeSeries(baselineTimeSeries, dataset.isAdditive());
        compareResult.setBaselineValue(baseLineval);
        compareResult.setCompareMode(compareMode);
        compareResult.setChange(calculateChange(currentVal, baseLineval));
        response.getCompareResults().add(compareResult);
      }
    } catch (Exception e) {
      LOG.error("Error fetching the timeseries data from pinot", e);
      throw new RuntimeException(e);
    }
    return response;
  }

  private double calculateChange(double currentValue, double baselineValue) {
    if (baselineValue == 0.0) {
      if (currentValue != 0) {
        return 1;  // 100 % change
      }
      if (currentValue == 0) {
        return 0;  // No change
      }
    }
    return (currentValue - baselineValue) / baselineValue;
  }

  double getTotalFromTimeSeries (MetricTimeSeries metricTimeSeries, boolean isAdditive) {
    double total = 0.0;

    // MetricTimeSeries will have multiple values in case of derived/multimetric
    Number[] metricTotals = metricTimeSeries.getMetricSums();
    total = metricTotals[0].doubleValue();

    if (!isAdditive) {
      // for non Additive data sets return the average
      total /= metricTimeSeries.getTimeWindowSet().size();
    }
    return total;
  }


  /**
   * Get count of anomalies for metric in time range
   * @param metricId
   * @param startTime
   * @param endTime
   * @return
   */
  @GET
  @Path("getAnomalyCount/{metricId}/{startTime}/{endTime}")
  public AnomaliesSummary getAnomalyCountForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) {
    AnomaliesSummary anomaliesSummary = new AnomaliesSummary();
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricIdInRange(metricId, startTime, endTime);

    int resolvedAnomalies = 0;
    int unresolvedAnomalies = 0;
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      AnomalyFeedback anomalyFeedback = mergedAnomaly.getFeedback();
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
   * Search anomalies only by time
   * @param startTime
   * @param endTime
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/time/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByTime(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber) throws Exception {

    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByTime(startTime, endTime);
    try {
      mergedAnomalies = AlertFilterHelper.applyFiltrationRule(mergedAnomalies, alertFilterFactory);
    } catch (Exception e) {
      LOG.warn(
          "Failed to apply alert filters on anomalies in start:{}, end:{}, exception:{}",
          new DateTime(startTime), new DateTime(endTime), e);
    }
    AnomaliesWrapper anomaliesWrapper = constructAnomaliesWrapperFromMergedAnomalies(mergedAnomalies, pageNumber);
    return anomaliesWrapper;
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
  @Path("search/anomalyIds/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByAnomalyIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber,
      @QueryParam("anomalyIds") String anomalyIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    String[] anomalyIds = anomalyIdsString.split(COMMA_SEPARATOR);
    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();
    for (String id : anomalyIds) {
      Long anomalyId = Long.valueOf(id);
      MergedAnomalyResultDTO anomaly = mergedAnomalyResultDAO.findById(anomalyId);
      if (anomaly != null) {
        mergedAnomalies.add(anomaly);
      }
    }
    AnomaliesWrapper anomaliesWrapper = constructAnomaliesWrapperFromMergedAnomalies(mergedAnomalies, pageNumber);
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
  @Path("search/dashboardId/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByDashboardId(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber,
      @QueryParam("dashboardId") String dashboardId,
      @QueryParam("functionName") String functionName) throws Exception {

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.findById(Long.valueOf(dashboardId));
    String metricIdsString = Joiner.on(COMMA_SEPARATOR).join(dashboardConfig.getMetricIds());
    return getAnomaliesByMetricIds(startTime, endTime, pageNumber, metricIdsString, functionName);
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
  @Path("search/metricIds/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByMetricIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber,
      @QueryParam("metricIds") String metricIdsString,
      @QueryParam("functionName") String functionName) throws Exception {

    String[] metricIdsList = metricIdsString.split(COMMA_SEPARATOR);
    List<Long> metricIds = new ArrayList<>();
    for (String metricId : metricIdsList) {
      metricIds.add(Long.valueOf(metricId));
    }
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricIdsInRange(metricIds, startTime, endTime);
    AnomaliesWrapper anomaliesWrapper = constructAnomaliesWrapperFromMergedAnomalies(mergedAnomalies, pageNumber);
    return anomaliesWrapper;
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
      AnomalyFeedback feedback = result.getFeedback();
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
   * Get anomalies for metric id in a time range
   * @param metricId
   * @param startTime
   * @param endTime
   * @return
   */
  private List<MergedAnomalyResultDTO> getAnomaliesForMetricIdInRange(Long metricId, Long startTime, Long endTime) {
    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
    String dataset = metricConfig.getDataset();
    String metric = metricConfig.getName();
    List<MergedAnomalyResultDTO> mergedAnomalies =
        mergedAnomalyResultDAO.findByCollectionMetricTime(dataset, metric, startTime, endTime, false);
    try {
      mergedAnomalies = AlertFilterHelper.applyFiltrationRule(mergedAnomalies, alertFilterFactory);
    } catch (Exception e) {
      LOG.warn(
          "Failed to apply alert filters on anomalies for metricid:{}, start:{}, end:{}, exception:{}",
          metricId, new DateTime(startTime), new DateTime(endTime), e);
    }
    return mergedAnomalies;
  }

  private List<MergedAnomalyResultDTO> getAnomaliesForMetricIdsInRange(List<Long> metricIds, Long startTime, Long endTime) {
    List<MergedAnomalyResultDTO> mergedAnomaliesForMetricIdsInRange = new ArrayList<>();
    for (Long metricId : metricIds) {
      List<MergedAnomalyResultDTO> filteredMergedAnomalies =
          getAnomaliesForMetricIdInRange(metricId, startTime, endTime);
      mergedAnomaliesForMetricIdsInRange.addAll(filteredMergedAnomalies);
    }
    return mergedAnomaliesForMetricIdsInRange;
  }


  /**
   * Extract data values form timeseries object
   * @param dataSeries
   * @return
   * @throws JSONException
   */
  private List<String> getDataFromTimeSeriesObject(List<Double> dataSeries) throws JSONException {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < dataSeries.size(); i++) {
        list.add(dataSeries.get(i).toString());
    }
    LOG.info("List {}", list);
    return list;
  }

  /**
   * Extract date values from time series object
   * @param timeBucket
   * @param timeSeriesDateFormatter
   * @return
   * @throws JSONException
   */
  private List<String> getDateFromTimeSeriesObject(List<TimeBucket> timeBucket, DateTimeFormatter timeSeriesDateFormatter) throws JSONException {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < timeBucket.size(); i++) {
      list.add(timeSeriesDateFormatter.print(timeBucket.get(i).getCurrentStart()));
    }
    LOG.info("List {}", list);
    return list;
  }

  /**
   * Get formatted date time for anomaly chart
   * @param timestamp
   * @param datasetConfig
   * @return
   */
  private String getFormattedDateTime(long timestamp, DatasetConfigDTO datasetConfig) {
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


  /**
   * We want the anomaly point to be in between the shaded anomaly region.
   * Hence we will append some buffer to the start and end of the anomaly point
   * @param startTime
   * @param dateTimeZone
   * @param dataUnit
   * @return new anomaly start time
   */
  private long appendRegionToAnomalyStart(long startTime, DateTimeZone dateTimeZone, TimeUnit dataUnit) {
    Period periodToAppend = new Period(0, 0, 0, 0, 0, 0, 0, 0);
    switch (dataUnit) {
      case DAYS: // append 1 HOUR
        periodToAppend = new Period(0, 0, 0, 0, 1, 0, 0, 0);
        break;
      case HOURS: // append 30 MINUTES
        periodToAppend = new Period(0, 0, 0, 0, 0, 30, 0, 0);
        break;
      default:
        break;
    }
    DateTime startDateTime = new DateTime(startTime, dateTimeZone);
    return startDateTime.minus(periodToAppend).getMillis();
  }

  /**
   * We want the anomaly point to be in between the shaded anomaly region.
   * Hence we will append some buffer to the start and end of the anomaly point
   * @param endTime
   * @param dateTimeZone
   * @param dataUnit
   * @return new anomaly start time
   */
  private long subtractRegionFromAnomalyEnd(long endTime, DateTimeZone dateTimeZone, TimeUnit dataUnit) {
    Period periodToSubtract = new Period(0, 0, 0, 0, 0, 0, 0, 0);
    switch (dataUnit) {
      case DAYS: // subtract 23 HOUR
        periodToSubtract = new Period(0, 0, 0, 0, 23, 0, 0, 0);
        break;
      case HOURS: // subtract 30 MINUTES
        periodToSubtract = new Period(0, 0, 0, 0, 0, 30, 0, 0);
        break;
      default:
        break;
    }
    DateTime endDateTime = new DateTime(endTime, dateTimeZone);
    return endDateTime.minus(periodToSubtract).getMillis();
  }


  /**
   * Constructs AnomaliesWrapper object from a list of merged anomalies
   * @param mergedAnomalies
   * @return
   * @throws ExecutionException
   */
  private AnomaliesWrapper constructAnomaliesWrapperFromMergedAnomalies(List<MergedAnomalyResultDTO> mergedAnomalies, int pageNumber) throws ExecutionException {
    AnomaliesWrapper anomaliesWrapper = new AnomaliesWrapper();
    anomaliesWrapper.setTotalAnomalies(mergedAnomalies.size());
    LOG.info("Total anomalies: {}", mergedAnomalies.size());


    // TODO: get page number and page size from client
    int pageSize = DEFAULT_PAGE_SIZE;
    int maxPageNumber = (mergedAnomalies.size() - 1) / pageSize + 1;
    if (pageNumber > maxPageNumber) {
      pageNumber = maxPageNumber;
    }
    if (pageNumber < 1) {
      pageNumber = 1;
    }

    int fromIndex = (pageNumber - 1) * pageSize;
    int toIndex = pageNumber * pageSize;
    if (toIndex > mergedAnomalies.size()) {
      toIndex = mergedAnomalies.size();
    }

    // Show most recent anomalies first, i.e., the anomaly whose end time is most recent then largest id shown at top
    Collections.sort(mergedAnomalies, Collections.reverseOrder(new MergedAnomalyEndTimeComparator()));

    List<MergedAnomalyResultDTO> displayedAnomalies = mergedAnomalies.subList(fromIndex, toIndex);
    anomaliesWrapper.setNumAnomaliesOnPage(displayedAnomalies.size());
    LOG.info("Page number: {} Page size: {} Num anomalies on page: {}", pageNumber, pageSize, displayedAnomalies.size());

    // for each anomaly, create anomaly details
    List<Future<AnomalyDetails>> anomalyDetailsListFutures = new ArrayList<>();
    for (final MergedAnomalyResultDTO mergedAnomaly : displayedAnomalies) {
      Callable<AnomalyDetails> callable = new Callable<AnomalyDetails>() {
        @Override
        public AnomalyDetails call() throws Exception {
          String dataset = mergedAnomaly.getCollection();
          DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);

          return getAnomalyDetails(mergedAnomaly, datasetConfig, getExternalURL(mergedAnomaly));
        }
      };
      anomalyDetailsListFutures.add(threadPool.submit(callable));
    }

    List<AnomalyDetails> anomalyDetailsList = new ArrayList<>();
    for (Future<AnomalyDetails> anomalyDetailsFuture : anomalyDetailsListFutures) {
      try {
        AnomalyDetails anomalyDetails = anomalyDetailsFuture.get(120, TimeUnit.SECONDS);
        if (anomalyDetails != null) {
          anomalyDetailsList.add(anomalyDetails);
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.error("Exception in getting AnomalyDetails", e);
      }
    }
    anomaliesWrapper.setAnomalyDetailsList(anomalyDetailsList);

    return anomaliesWrapper;
  }

  /**
   * Return the natural order of MergedAnomaly by comparing their anomaly's end time.
   * The tie breaker is their anomaly ID.
   */
  private static class MergedAnomalyEndTimeComparator implements Comparator<MergedAnomalyResultDTO> {
    @Override
    public int compare(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2) {
      if (anomaly1.getEndTime() != anomaly2.getEndTime()) {
        return (int) (anomaly1.getEndTime() - anomaly2.getEndTime());
      } else {
        return (int) (anomaly1.getId() - anomaly2.getId());
      }
    }
  }

  private String getExternalURL(MergedAnomalyResultDTO mergedAnomaly) {

    String metric = mergedAnomaly.getMetric();
    String dataset = mergedAnomaly.getCollection();
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metric, dataset);
    Map<String, String> urlTemplates = metricConfigDTO.getExtSourceLinkInfo();
    if (MapUtils.isEmpty(urlTemplates)) {
      return "";
    }

    // Construct context for substituting the keywords in URL template
    Map<String, String> context = new HashMap<>();
    // context for each pair of dimension name and value
    if (MapUtils.isNotEmpty(mergedAnomaly.getDimensions())) {
      for (Map.Entry<String, String> entry : mergedAnomaly.getDimensions().entrySet()) {
        // TODO: Change to case sensitive?
        try {
          String URLEncodedDimensionName = URLEncoder.encode(entry.getKey().toLowerCase(), "UTF-8");
          String URLEncodedDimensionValue = URLEncoder.encode(entry.getValue().toLowerCase(), "UTF-8");
          context.put(URLEncodedDimensionName, URLEncodedDimensionValue);
        } catch (UnsupportedEncodingException e) {
          LOG.warn("Unable to encode this dimension pair {}:{} for external links.", entry.getKey(), entry.getValue());
        }
      }
    }

    Long startTime = mergedAnomaly.getStartTime();
    Long endTime = mergedAnomaly.getEndTime();
    Map<String, String> externalLinkTimeGranularity = metricConfigDTO.getExtSourceLinkTimeGranularity();
    for (Map.Entry<String, String> externalLinkEntry : urlTemplates.entrySet()) {
      String sourceName = externalLinkEntry.getKey();
      String urlTemplate = externalLinkEntry.getValue();
      // context for startTime and endTime
      putExternalLinkTimeContext(startTime, endTime, sourceName, context, externalLinkTimeGranularity);

      StrSubstitutor strSubstitutor = new StrSubstitutor(context);
      String extSourceUrl = strSubstitutor.replace(urlTemplate);
      urlTemplates.put(sourceName, extSourceUrl);
    }
    return new JSONObject(urlTemplates).toString();
  }

  /**
   * The default time granularity of ThirdEye is MILLISECONDS; however, it could be different for external links. This
   * method updates the start and end time according to external link's time granularity. If a link's granularity is
   * not given, then the default granularity (MILLISECONDS) is used.
   *
   * @param startTime the start time in milliseconds/
   * @param endTime the end time in milliseconds.
   * @param linkName the name of the external link.
   * @param context the context to be updated, which is used by StrSubstitutor.
   * @param externalLinkTimeGranularity the granularity of the external link.
   */
  private void putExternalLinkTimeContext(long startTime, long endTime, String linkName,
      Map<String, String> context, Map<String, String> externalLinkTimeGranularity) {
    if (MapUtils.isNotEmpty(externalLinkTimeGranularity) && externalLinkTimeGranularity.containsKey(linkName)) {
      String timeGranularityString = externalLinkTimeGranularity.get(linkName);
      TimeGranularity timeGranularity = TimeGranularity.fromString(timeGranularityString);
      context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(timeGranularity.convertToUnit(startTime)));
      context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(timeGranularity.convertToUnit(endTime)));
    } else { // put start and end time as is
      context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(startTime));
      context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(endTime));
    }
  }

  /**
   * Generates Anomaly Details for each merged anomaly
   * @param mergedAnomaly
   * @param datasetConfig
   * @param externalUrl
   * @return
   */
  private AnomalyDetails getAnomalyDetails(MergedAnomalyResultDTO mergedAnomaly, DatasetConfigDTO datasetConfig,
      String externalUrl) throws Exception {

    String dataset = datasetConfig.getDataset();
    String metricName = mergedAnomaly.getMetric();

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(mergedAnomaly.getFunctionId());
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    String aggGranularity = datasetConfig.bucketTimeGranularity().toAggregationGranularityString();

    // get anomaly window range - this is the data to fetch (anomaly region + some offset if required)
    // the function will tell us this range, as how much data we fetch can change depending on which function is being executed
    TimeRange anomalyWindowRange = getAnomalyWindowOffset(mergedAnomaly, anomalyFunction, datasetConfig);
    long anomalyWindowStart = anomalyWindowRange.getStart();
    long anomalyWindowEnd = anomalyWindowRange.getEnd();

    DimensionMap dimensions = mergedAnomaly.getDimensions();
    TimeGranularity timeGranularity =
        Utils.getAggregationTimeGranularity(aggGranularity, anomalyFunctionSpec.getCollection());
    long bucketMillis = timeGranularity.toMillis();
    AnomalyDetails anomalyDetails = null;
    try {
      AnomalyTimelinesView anomalyTimelinesView = null;
      Map<String, String> anomalyProps = mergedAnomaly.getProperties();
      if(anomalyProps.containsKey("anomalyTimelinesView")) {
        anomalyTimelinesView = AnomalyTimelinesView.fromJsonString(anomalyProps.get("anomalyTimelinesView"));
      } else {
        AnomalyDetectionInputContext adInputContext =
            TimeBasedAnomalyMerger.fetchDataByDimension(anomalyWindowStart, anomalyWindowEnd, dimensions,
                anomalyFunction, mergedAnomalyResultDAO, overrideConfigDAO, true);

        MetricTimeSeries metricTimeSeries = adInputContext.getDimensionKeyMetricTimeSeriesMap().get(dimensions);

        // Transform time series with scaling factor
        List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
        if (CollectionUtils.isNotEmpty(scalingFactors)) {
          Properties properties = anomalyFunction.getProperties();
          MetricTransfer.rescaleMetric(metricTimeSeries, anomalyWindowStart, scalingFactors, anomalyFunctionSpec.getTopicMetric(), properties);
        }

        List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
        // check if there is AnomalyTimelinesView in the Properties. If yes, use the AnomalyTimelinesView
        if (anomalyProps.containsKey("anomalyTimelinesView")) {
          anomalyTimelinesView = AnomalyTimelinesView.fromJsonString(anomalyProps.get("anomalyTimelinesView"));
        } else {
          // Known anomalies are ignored (the null parameter) because 1. we can reduce users' waiting time and 2. presentation
          // data does not need to be as accurate as the one used for detecting anomalies
          anomalyTimelinesView = anomalyFunction.getTimeSeriesView(metricTimeSeries, bucketMillis, anomalyFunctionSpec.getTopicMetric(),
              anomalyWindowStart, anomalyWindowEnd, knownAnomalies);
        }
      }

      // get viewing window range - this is the region to display along with anomaly, from all the fetched data.
      // the function will tell us this range, as how much data we display can change depending on which function is being executed
      TimeRange viewWindowRange = getViewWindowOffset(mergedAnomaly, anomalyFunction, datasetConfig);
      long viewWindowStart = viewWindowRange.getStart();
      long viewWindowEnd = viewWindowRange.getEnd();
      anomalyDetails = constructAnomalyDetails(metricName, dataset, datasetConfig, mergedAnomaly, anomalyFunctionSpec,
          viewWindowStart, viewWindowEnd, anomalyTimelinesView, externalUrl);
    } catch (Exception e) {
      LOG.error("Exception in constructing anomaly wrapper for anomaly {}", mergedAnomaly.getId(), e);
    }
    return anomalyDetails;
  }

  /** Construct anomaly details using all details fetched from calls
   *
   * @param metricName
   * @param dataset
   * @param datasetConfig
   * @param mergedAnomaly
   * @param anomalyFunction
   * @param currentStartTime inclusive
   * @param currentEndTime inclusive
   * @param anomalyTimelinesView
   * @return
   * @throws JSONException
   */
  private AnomalyDetails constructAnomalyDetails(String metricName, String dataset, DatasetConfigDTO datasetConfig,
      MergedAnomalyResultDTO mergedAnomaly, AnomalyFunctionDTO anomalyFunction, long currentStartTime,
      long currentEndTime, AnomalyTimelinesView anomalyTimelinesView, String externalUrl)
      throws JSONException {

    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metricName, dataset);
    DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
    TimeUnit dataUnit = datasetConfig.bucketTimeGranularity().getUnit();

    AnomalyDetails anomalyDetails = new AnomalyDetails();
    anomalyDetails.setMetric(metricName);
    anomalyDetails.setDataset(dataset);

    if (metricConfigDTO != null) {
      anomalyDetails.setMetricId(metricConfigDTO.getId());
    }
    anomalyDetails.setTimeUnit(dataUnit.toString());

    // The filter ensures that the returned time series from anomalies function only includes the values that are
    // located inside the request windows (i.e., between currentStartTime and currentEndTime, inclusive).
    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    int timeStartIndex = -1;
    int timeEndIndex = -1;
    for (int i = 0; i < timeBuckets.size(); ++i) {
      long currentTimeStamp = timeBuckets.get(i).getCurrentStart();
      if (timeStartIndex < 0 &&  currentTimeStamp >= currentStartTime) {
        timeStartIndex = i;
        timeEndIndex = i + 1;
      } else if (currentTimeStamp <= currentEndTime) {
        timeEndIndex = i + 1;
      } else if (currentTimeStamp > currentEndTime) {
        break;
      }
    }
    if (timeStartIndex < 0 || timeEndIndex < 0) {
      timeStartIndex = 0;
      timeEndIndex = 0;
    }

    // get this from timeseries calls
    List<String> dateValues = getDateFromTimeSeriesObject(timeBuckets.subList(timeStartIndex, timeEndIndex), timeSeriesDateFormatter);
    anomalyDetails.setDates(dateValues);
    anomalyDetails.setCurrentStart(getFormattedDateTime(currentStartTime, datasetConfig));
    anomalyDetails.setCurrentEnd(getFormattedDateTime(currentEndTime, datasetConfig));
    List<String> baselineValues = getDataFromTimeSeriesObject(anomalyTimelinesView.getBaselineValues().subList(timeStartIndex, timeEndIndex));
    anomalyDetails.setBaselineValues(baselineValues);
    List<String> currentValues = getDataFromTimeSeriesObject(anomalyTimelinesView.getCurrentValues().subList(timeStartIndex, timeEndIndex));
    anomalyDetails.setCurrentValues(currentValues);

    // from function and anomaly
    anomalyDetails.setAnomalyId(mergedAnomaly.getId());
    anomalyDetails.setAnomalyStart(timeSeriesDateFormatter.print(mergedAnomaly.getStartTime()));
    anomalyDetails.setAnomalyEnd(timeSeriesDateFormatter.print(mergedAnomaly.getEndTime()));
    long newAnomalyRegionStart = appendRegionToAnomalyStart(mergedAnomaly.getStartTime(), dateTimeZone, dataUnit);
    long newAnomalyRegionEnd = subtractRegionFromAnomalyEnd(mergedAnomaly.getEndTime(), dateTimeZone, dataUnit);
    anomalyDetails.setAnomalyRegionStart(timeSeriesDateFormatter.print(newAnomalyRegionStart));
    anomalyDetails.setAnomalyRegionEnd(timeSeriesDateFormatter.print(newAnomalyRegionEnd));
    anomalyDetails.setCurrent(ThirdEyeUtils.getRoundedValue(mergedAnomaly.getAvgCurrentVal()));
    anomalyDetails.setBaseline(ThirdEyeUtils.getRoundedValue(mergedAnomaly.getAvgBaselineVal()));
    anomalyDetails.setAnomalyFunctionId(anomalyFunction.getId());
    anomalyDetails.setAnomalyFunctionName(anomalyFunction.getFunctionName());
    anomalyDetails.setAnomalyFunctionType(anomalyFunction.getType());
    anomalyDetails.setAnomalyFunctionProps(anomalyFunction.getProperties());
    anomalyDetails.setAnomalyFunctionDimension(mergedAnomaly.getDimensions().toString());
    if (mergedAnomaly.getFeedback() != null) {
      anomalyDetails.setAnomalyFeedback(AnomalyDetails.getFeedbackStringFromFeedbackType(mergedAnomaly.getFeedback().getFeedbackType()));
    }
    anomalyDetails.setExternalUrl(externalUrl);
    return anomalyDetails;
  }


  private TimeRange getAnomalyWindowOffset(MergedAnomalyResultDTO mergedAnomaly, BaseAnomalyFunction anomalyFunction,
      DatasetConfigDTO datasetConfig) {
    AnomalyOffset anomalyWindowOffset = anomalyFunction.getAnomalyWindowOffset(datasetConfig);
    TimeRange anomalyWindowRange = getAnomalyTimeRangeWithOffsets(anomalyWindowOffset, mergedAnomaly, datasetConfig);
    return anomalyWindowRange;
  }


  private TimeRange getViewWindowOffset(MergedAnomalyResultDTO mergedAnomaly, BaseAnomalyFunction anomalyFunction,
      DatasetConfigDTO datasetConfig) {
    AnomalyOffset viewWindowOffset = anomalyFunction.getViewWindowOffset(datasetConfig);
    TimeRange viewWindowRange = getAnomalyTimeRangeWithOffsets(viewWindowOffset, mergedAnomaly, datasetConfig);
    return viewWindowRange;
  }

  private TimeRange getAnomalyTimeRangeWithOffsets(AnomalyOffset offset, MergedAnomalyResultDTO mergedAnomaly,
      DatasetConfigDTO datasetConfig) {
    long anomalyStartTime = mergedAnomaly.getStartTime();
    long anomalyEndTime = mergedAnomaly.getEndTime();
    Period preOffsetPeriod = offset.getPreOffsetPeriod();
    Period postOffsetPeriod = offset.getPostOffsetPeriod();

    DateTimeZone dateTimeZone = DateTimeZone.forID(datasetConfig.getTimezone());
    DateTime anomalyStartDateTime = new DateTime(anomalyStartTime, dateTimeZone);
    DateTime anomalyEndDateTime = new DateTime(anomalyEndTime, dateTimeZone);
    anomalyStartDateTime = anomalyStartDateTime.minus(preOffsetPeriod);
    anomalyEndDateTime = anomalyEndDateTime.plus(postOffsetPeriod);
    anomalyStartTime = anomalyStartDateTime.getMillis();
    anomalyEndTime = anomalyEndDateTime.getMillis();
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

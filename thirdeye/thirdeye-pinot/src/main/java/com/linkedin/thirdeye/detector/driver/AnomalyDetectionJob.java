package com.linkedin.thirdeye.detector.driver;

import static com.linkedin.thirdeye.detector.driver.FailureEmailConfiguration.FAILURE_EMAIL_CONFIG_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.entity.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.lib.util.JobUtils;

public class AnomalyDetectionJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJob.class);
  public static final String FUNCTION = "FUNCTION";
  public static final String TIME_SERIES_HANDLER = "TIME_SERIES_HANDLER";
  public static final String TIME_SERIES_RESPONSE_CONVERTER = "TIME_SERIES_RESPONSE_CONVERTER";
  public static final String RESULT_DAO = "RESULT_DAO";
  public static final String SESSION_FACTORY = "SESSION_FACTORY";
  public static final String WINDOW_END = "WINDOW_END";
  public static final String WINDOW_START = "WINDOW_START";
  public static final String METRIC_REGISTRY = "METRIC_REGISTRY";
  public static final String RELATION_DAO = "RELATION_DAO";

  private AnomalyFunction anomalyFunction;
  private TimeSeriesHandler timeSeriesHandler;
  private TimeSeriesResponseConverter timeSeriesResponseConverter;
  private AnomalyResultDAO resultDAO;
  private AnomalyFunctionRelationDAO relationDAO;
  private SessionFactory sessionFactory;
  private MetricRegistry metricRegistry;
  private Histogram histogram;
  private String collection;
  private List<String> collectionDimensions;
  private MetricFunction metricFunction;
  private DateTime windowStart;
  private DateTime windowEnd;
  private List<AnomalyResult> knownAnomalies;
  private int anomalyCounter;

  @Override
  public void execute(final JobExecutionContext context) throws JobExecutionException {
    anomalyFunction = (AnomalyFunction) context.getJobDetail().getJobDataMap().get(FUNCTION);
    final FailureEmailConfiguration failureEmailConfig =
        (FailureEmailConfiguration) context.getJobDetail().getJobDataMap()
            .get(FAILURE_EMAIL_CONFIG_KEY);
    try {
      run(context, anomalyFunction);
    } catch (Throwable t) {
      AnomalyFunctionSpec spec = anomalyFunction.getSpec();
      LOG.error("Job failed with exception:", t);
      long id = spec.getId();
      String collection = spec.getCollection();
      String metric = spec.getMetric();

      String subject =
          String.format("FAILED ANOMALY DETECTION JOB ID=%d (%s:%s)", id, collection, metric);
      String textBody =
          String.format("%s%n%nException:%s", spec.toString(), ExceptionUtils.getStackTrace(t));
      try {
        JobUtils.sendFailureEmail(failureEmailConfig, subject, textBody);
      } catch (EmailException e) {
        throw new JobExecutionException(e);
      }
    }
  }

  private void run(JobExecutionContext context, AnomalyFunction anomalyFunction)
      throws JobExecutionException {

    // thirdEyeClient = (ThirdEyeClient) context.getJobDetail().getJobDataMap().get(CLIENT);
    LOG.info("AnomalyFunction: {}", anomalyFunction);
    AnomalyFunctionSpec spec = anomalyFunction.getSpec();
    LOG.info("AnomalyFunctionSpec: {}", spec);
    timeSeriesHandler =
        (TimeSeriesHandler) context.getJobDetail().getJobDataMap().get(TIME_SERIES_HANDLER);
    timeSeriesResponseConverter =
        (TimeSeriesResponseConverter) context.getJobDetail().getJobDataMap()
            .get(TIME_SERIES_RESPONSE_CONVERTER);
    resultDAO = (AnomalyResultDAO) context.getJobDetail().getJobDataMap().get(RESULT_DAO);
    relationDAO =
        (AnomalyFunctionRelationDAO) context.getJobDetail().getJobDataMap().get(RELATION_DAO);
    sessionFactory = (SessionFactory) context.getJobDetail().getJobDataMap().get(SESSION_FACTORY);
    metricRegistry = (MetricRegistry) context.getJobDetail().getJobDataMap().get(METRIC_REGISTRY);
    String windowEndProp = context.getJobDetail().getJobDataMap().getString(WINDOW_END);
    String windowStartProp = context.getJobDetail().getJobDataMap().getString(WINDOW_START);

    // Get histogram for this job execution time
    String histogramName = context.getJobDetail().getKey().getName();
    histogram = metricRegistry.getHistograms().get(histogramName);
    if (histogram == null) {
      histogram = metricRegistry.histogram(histogramName);
    }

    // Compute window end
    if (windowEndProp == null) {
      long delayMillis = 0;
      if (spec.getWindowDelay() != null) {
        delayMillis =
            TimeUnit.MILLISECONDS.convert(spec.getWindowDelay(), spec.getWindowDelayUnit());
      }
      Date scheduledFireTime = context.getScheduledFireTime();

      windowEnd = new DateTime(scheduledFireTime).minus(delayMillis);
      LOG.info(
          "Running anomaly detection job with scheduledFireTime: {}, delayMillis: {}, windowEnd: {}",
          scheduledFireTime, delayMillis, windowEnd);
    } else {
      windowEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(windowEndProp);
    }

    // Compute window start
    if (windowStartProp == null) {
      int windowSize = spec.getWindowSize();
      TimeUnit windowUnit = spec.getWindowUnit();
      long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
      windowStart = windowEnd.minus(windowMillis);
      LOG.info(
          "Running anomaly detection job with windowUnit: {}, windowMillis: {}, windowStart: {}",
          windowUnit, windowMillis, windowStart);
    } else {
      windowStart = ISODateTimeFormat.dateTimeParser().parseDateTime(windowStartProp);
    }

    // Compute metric function
    TimeGranularity timeGranularity =
        new TimeGranularity(spec.getBucketSize(), spec.getBucketUnit());

    metricFunction = new MetricFunction(spec.getMetricFunction(), spec.getMetric());

    // Collection
    collection = spec.getCollection();

    LOG.info("Running anomaly detection job with metricFunction: {}, collection: {}",
        metricFunction, collection);

    try {
      CollectionSchema collectionSchema =
          timeSeriesHandler.getClient().getCollectionSchema(collection);
      collectionDimensions = collectionSchema.getDimensionNames();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    // Get existing anomalies for this time range
    knownAnomalies = getExistingAnomalies();
    Queue<TimeSeriesRequest> timeSeriesRequestQueue =
        getTimeSeriesRequests(spec, windowEndProp, windowStartProp, timeGranularity);

    while (!timeSeriesRequestQueue.isEmpty()) {
      try {
        List<TimeSeriesRequest> nextRequests = exploreCombination(timeSeriesRequestQueue.remove());
        // TODO support further/deeper exploration
        // timeSeriesRequestQueue.addAll(nextRequests);
      } catch (Exception e) {
        throw new JobExecutionException(e);
      }
    }
    LOG.info("{} anomalies found in total", anomalyCounter);
  }

  private Queue<TimeSeriesRequest> getTimeSeriesRequests(AnomalyFunctionSpec spec,
      String windowEndProp, String windowStartProp, TimeGranularity timeGranularity) {
    // Seed request with top-level...
    Queue<TimeSeriesRequest> timeSeriesRequestQueue = new LinkedList<>();
    TimeSeriesRequest topLevelRequest = new TimeSeriesRequest();
    topLevelRequest.setCollectionName(collection);
    List<MetricFunction> metricFunctions = Collections.singletonList(metricFunction);
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metricFunctions);
    topLevelRequest.setMetricExpressions(metricExpressions);
    topLevelRequest.setAggregationTimeGranularity(timeGranularity);
    topLevelRequest.setStart(windowStart);
    topLevelRequest.setEnd(windowEnd);
    topLevelRequest.setEndDateInclusive(false);

    LOG.info(
        "Running anomaly detection job with windowStartProp: {}, windowEndProp: {}, metricExpressions: {}, timeGranularity: {}, windowStart: {}, windowEnd: {}",
        windowStartProp, windowEndProp, metricExpressions, timeGranularity, windowStart, windowEnd);

    // Filters are supported now. Now filter clauses can be specified in AnomalyFunctionSpec.
    String filters = spec.getFilters();
    if (StringUtils.isNotBlank(filters)) {
      topLevelRequest.setFilterSet(spec.getFilterSet());
    }
    String exploreDimensionsString = spec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      timeSeriesRequestQueue.add(topLevelRequest);
    } else {
      // And all the dimensions which we should explore
      List<String> exploreDimensions = Arrays.asList(exploreDimensionsString.split(","));
      // Technically we should be able to pass in all exploration dimensions in one go and let the
      // handler split the requests sent to the underlying client, but we explicitly split here to
      // mimic previous behavior before the client api changes.
      // TimeSeriesRequest groupByRequest = new TimeSeriesRequest(topLevelRequest);
      // groupByRequest.setGroupByDimensions(exploreDimensions);
      for (String exploreDimension : exploreDimensions) {
        TimeSeriesRequest groupByRequest = new TimeSeriesRequest(topLevelRequest);
        groupByRequest.setGroupByDimensions(Collections.singletonList(exploreDimension));
        timeSeriesRequestQueue.add(groupByRequest);
      }
    }
    return timeSeriesRequestQueue;
  }

  private List<TimeSeriesRequest> exploreCombination(TimeSeriesRequest request) throws Exception {
    LOG.info("Exploring {}", request);

    // Query server
    TimeSeriesResponse response;
    try {
      LOG.debug("Executing {}", request);
      response = timeSeriesHandler.handle(request);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    Map<DimensionKey, MetricTimeSeries> res =
        timeSeriesResponseConverter.toMap(response, collectionDimensions);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      if (entry.getValue().getTimeWindowSet().size() < 2) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", entry.getKey());
        continue;
      }

      try {
        // Run algorithm
        long startTime = System.currentTimeMillis();
        DimensionKey dimensionKey = entry.getKey();
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info(
            "Analyzing anomaly function with dimensionKey: {}, windowStart: {}, windowEnd: {}",
            dimensionKey, windowStart, windowEnd);
        List<AnomalyResult> results =
            anomalyFunction.analyze(dimensionKey, metricTimeSeries, windowStart, windowEnd,
                knownAnomalies);

        long endTime = System.currentTimeMillis();
        LOG.info("Updating histogram with startTime: {}, endTime: {}", startTime, endTime);
        histogram.update(endTime - startTime);

        // Handle results
        handleResults(results);

        // Remove any known anomalies
        results.removeAll(knownAnomalies);

        LOG.info("{} has {} anomalies in window {} to {}", entry.getKey(), results.size(),
            windowStart, windowEnd);
        anomalyCounter += results.size();
      } catch (Exception e) {
        LOG.error("Could not compute for {}", entry.getKey(), e);
      }
    }

    return ImmutableList.of(); // TODO provide a more advanced exploration policy
  }

  private List<AnomalyResult> getExistingAnomalies() {
    List<AnomalyResult> results = new ArrayList<>();

    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        // The ones for this function
        results.addAll(resultDAO.findAllByCollectionTimeAndFunction(collection, windowStart,
            windowEnd, anomalyFunction.getSpec().getId()));

        // The ones for any related functions
        List<AnomalyFunctionRelation> relations =
            relationDAO.findByParent(anomalyFunction.getSpec().getId());
        for (AnomalyFunctionRelation relation : relations) {
          results.addAll(resultDAO.findAllByCollectionTimeAndFunction(collection, windowStart,
              windowEnd, relation.getChildId()));
        }

        transaction.commit();
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return results;
  }

  private void handleResults(List<AnomalyResult> results) {
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        for (AnomalyResult result : results) {
          // Properties that always come from the function spec
          AnomalyFunctionSpec spec = anomalyFunction.getSpec();
          result.setFunctionId(spec.getId());
          result.setFunctionType(spec.getType());
          result.setFunctionProperties(spec.getProperties());
          result.setCollection(spec.getCollection());
          result.setMetric(spec.getMetric());
          result.setFilters(spec.getFilters());

          // make sure score and weight are valid numbers
          result.setScore(normalize(result.getScore()));
          result.setWeight(normalize(result.getWeight()));
          resultDAO.createOrUpdate(result);
        }
        transaction.commit();
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }

  /** Handle any infinite or NaN values by replacing them with +/- max value or 0 */
  private double normalize(double value) {
    if (Double.isInfinite(value)) {
      return (value > 0.0 ? 1 : -1) * Double.MAX_VALUE;
    } else if (Double.isNaN(value)) {
      return 0.0; // default?
    } else {
      return value;
    }
  }

}

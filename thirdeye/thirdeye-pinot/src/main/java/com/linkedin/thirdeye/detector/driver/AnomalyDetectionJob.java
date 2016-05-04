package com.linkedin.thirdeye.detector.driver;

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
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;

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
  public void execute(JobExecutionContext context) throws JobExecutionException {
    anomalyFunction = (AnomalyFunction) context.getJobDetail().getJobDataMap().get(FUNCTION);
    // thirdEyeClient = (ThirdEyeClient) context.getJobDetail().getJobDataMap().get(CLIENT);
    AnomalyFunctionSpec spec = anomalyFunction.getSpec();
    timeSeriesHandler =
        (TimeSeriesHandler) context.getJobDetail().getJobDataMap().get(TIME_SERIES_HANDLER);
    timeSeriesResponseConverter = (TimeSeriesResponseConverter) context.getJobDetail()
        .getJobDataMap().get(TIME_SERIES_RESPONSE_CONVERTER);
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
        delayMillis = TimeUnit.MILLISECONDS.convert(spec.getWindowDelay(), spec.getWindowUnit());
      }
      Date scheduledFireTime = context.getScheduledFireTime();

      windowEnd = new DateTime(scheduledFireTime).minus(delayMillis);
    } else {
      windowEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(windowEndProp);
    }

    // Compute window start
    if (windowStartProp == null) {
      long windowMillis = TimeUnit.MILLISECONDS.convert(spec.getWindowSize(), spec.getWindowUnit());
      windowStart = windowEnd.minus(windowMillis);
    } else {
      windowStart = ISODateTimeFormat.dateTimeParser().parseDateTime(windowStartProp);
    }

    // Compute metric function
    TimeGranularity timeGranularity =
        new TimeGranularity(spec.getBucketSize(), spec.getBucketUnit());
    // TODO put sum into the function config
    metricFunction = new MetricFunction(MetricFunction.SUM, spec.getMetric());

    // Collection
    collection = spec.getCollection();

    try {
      CollectionSchema collectionSchema =
          timeSeriesHandler.getClient().getCollectionSchema(collection);
      collectionDimensions = collectionSchema.getDimensionNames();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    // Get existing anomalies for this time range
    knownAnomalies = getExistingAnomalies();

    // Seed request with top-level...
    Queue<TimeSeriesRequest> timeSeriesRequestQueue = new LinkedList<>();
    TimeSeriesRequest topLevelRequest = new TimeSeriesRequest();
    topLevelRequest.setCollectionName(collection);
    topLevelRequest.setMetricFunctions(Collections.singletonList(metricFunction));
    topLevelRequest.setAggregationTimeGranularity(timeGranularity);
    topLevelRequest.setStart(windowStart);
    topLevelRequest.setEnd(windowEnd);

    // Filters are supported now. Now filter clauses can be specified in AnomalyFunctionSpec.
    String filters = spec.getFilters();
    if (StringUtils.isNotBlank(filters)) {
      topLevelRequest.setFilterSet(spec.getFilterSet());
    }

    timeSeriesRequestQueue.add(topLevelRequest);

    // And all the dimensions which we should explore
    String exploreDimensionsString = spec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimensionsString)) {
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
        List<AnomalyResult> results = anomalyFunction.analyze(entry.getKey(), entry.getValue(),
            windowStart, windowEnd, knownAnomalies);
        long endTime = System.currentTimeMillis();
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
          result.setFunctionId(anomalyFunction.getSpec().getId());
          result.setFunctionType(anomalyFunction.getSpec().getType());
          result.setFunctionProperties(anomalyFunction.getSpec().getProperties());
          resultDAO.create(result);
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

}

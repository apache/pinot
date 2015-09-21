package com.linkedin.thirdeye.driver;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.function.AnomalyFunction;
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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectionJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJob.class);
  public static final String FUNCTION = "FUNCTION";
  public static final String CLIENT = "CLIENT";
  public static final String RESULT_DAO = "RESULT_DAO";
  public static final String SESSION_FACTORY = "SESSION_FACTORY";
  public static final String WINDOW_END = "WINDOW_END";

  private AnomalyFunction anomalyFunction;
  private ThirdEyeClient thirdEyeClient;
  private AnomalyResultDAO resultDAO;
  private SessionFactory sessionFactory;
  private String collection;
  private String metricFunction;
  private DateTime windowStart;
  private DateTime windowEnd;
  private List<AnomalyResult> knownAnomalies;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    anomalyFunction = (AnomalyFunction) context.getJobDetail().getJobDataMap().get(FUNCTION);
    thirdEyeClient = (ThirdEyeClient) context.getJobDetail().getJobDataMap().get(CLIENT);
    resultDAO = (AnomalyResultDAO) context.getJobDetail().getJobDataMap().get(RESULT_DAO);
    sessionFactory = (SessionFactory) context.getJobDetail().getJobDataMap().get(SESSION_FACTORY);
    String windowEndProp = context.getJobDetail().getJobDataMap().getString(WINDOW_END);

    // Compute window end
    if (windowEndProp == null) {
      long delayMillis = 0;
      if (anomalyFunction.getSpec().getWindowDelay() != null) {
        delayMillis = TimeUnit.MILLISECONDS.convert(
            anomalyFunction.getSpec().getWindowDelay(),
            anomalyFunction.getSpec().getWindowUnit());
      }
      windowEnd = DateTime.now().minus(delayMillis);
    } else {
      windowEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(windowEndProp);
    }

    // Compute window start
    long windowMillis = TimeUnit.MILLISECONDS.convert(
        anomalyFunction.getSpec().getWindowSize(),
        anomalyFunction.getSpec().getWindowUnit());
    windowStart = windowEnd.minus(windowMillis);

    // Compute metric function
    metricFunction = String.format("AGGREGATE_%d_%s(%s)",
        anomalyFunction.getSpec().getBucketSize(),
        anomalyFunction.getSpec().getBucketUnit(),
        anomalyFunction.getSpec().getMetric());

    // Collection
    collection = anomalyFunction.getSpec().getCollection();

    // Get existing anomalies for this time range
    knownAnomalies = getExistingAnomalies();

    // Seed request with top-level...
    Queue<ThirdEyeRequest> queue = new LinkedList<>();
    ThirdEyeRequest req = new ThirdEyeRequest()
        .setCollection(anomalyFunction.getSpec().getCollection())
        .setMetricFunction(metricFunction)
        .setStartTime(windowStart)
        .setEndTime(windowEnd);
    queue.add(req);

    // And all the dimensions which we should explore
    String exploreDimensionsString = anomalyFunction.getSpec().getExploreDimensions();
    if (exploreDimensionsString != null) {
      String[] exploreDimensions = exploreDimensionsString.split(",");
      for (String exploreDimension : exploreDimensions) {
        ThirdEyeRequest groupByReq = new ThirdEyeRequest(req);
        groupByReq.setGroupBy(exploreDimension);
        queue.add(groupByReq);
      }
    }

    while (!queue.isEmpty()) {
      try {
        List<ThirdEyeRequest> nextRequests = exploreCombination(queue.remove());
      } catch (Exception e) {
        throw new JobExecutionException(e);
      }
    }
  }

  private List<ThirdEyeRequest> exploreCombination(ThirdEyeRequest request) throws Exception {
    LOG.info("Exploring {}", request);

    // Query server
    Map<DimensionKey, MetricTimeSeries> res;
    try {
      LOG.debug("Executing {}", request);
      res = thirdEyeClient.execute(request);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      try {
        // Run algorithm
        List<AnomalyResult> results = anomalyFunction.analyze(
            entry.getKey(),
            entry.getValue(),
            windowStart,
            windowEnd,
            knownAnomalies);

        // Handle results
        handleResults(results);

        // Remove any known anomalies
        results.removeAll(knownAnomalies);

        if (!results.isEmpty()) {
          LOG.info("{} has {} anomalies in window {} to {}", entry.getKey(), results.size(), windowStart, windowEnd);
        }
      } catch (Exception e) {
        LOG.error("Could not compute for {}", entry.getKey(), e);
      }
    }

    return ImmutableList.of(); // TODO use # anomaly results or something to explore further
  }

  private List<AnomalyResult> getExistingAnomalies() {
    List<AnomalyResult> results = null;

    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        results = resultDAO.findAllByCollectionTimeAndFunction(
            collection, windowStart, windowEnd, anomalyFunction.getSpec().getId());
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
